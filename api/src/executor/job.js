const logger = require("logplease").create("executor/job");
const { v4: uuidv4 } = require("uuid");
const cp = require("child_process");
const path = require("path");
const config = require("../config");
const globals = require("../globals");
const fs = require("fs/promises");

const job_states = {
  READY: Symbol("Ready to be primed"),
  PRIMED: Symbol("Primed and ready for execution"),
  EXECUTED: Symbol("Executed and ready for cleanup"),
};

let uid = 0;
let gid = 0;

const Verdict = {
  AC: "AC",
  WA: "WA",
  COMPILATION: "COMPILATION",
  RUNTIME: "RUNTIME",
  TLE: "TLE",
  MLE: "MLE",
  PENDING: "PENDING",
  ERROR: "ERROR",
};

class Job {
  constructor({
    runtime,
    files,
    args,
    stdin,
    expected_output,
    timeouts,
    main,
    alias,
  }) {
    this.uuid = uuidv4();
    this.runtime = runtime;
    this.files = files;
    this.args = args;
    this.stdin = stdin;
    this.expected_output = expected_output;
    this.timeouts = timeouts;
    this.main = main;
    this.alias = alias;

    let file_list = this.files.map((f) => f.name);

    if (!file_list.includes(this.main)) {
      throw new Error(`Main file "${this.main}" will not be written to disk`);
    }

    this.shuffle_ids();

    this.state = job_states.READY;
    this.dir = path.join(
      config.data_directory,
      globals.data_directories.jobs,
      this.uuid
    );
  }

  async prime() {
    logger.info(`Priming job uuid=${this.uuid}`);

    logger.debug("Writing files to job cache");

    logger.debug(`Transfering ownership uid=${this.uid} gid=${this.gid}`);

    await fs.mkdir(this.dir, { mode: 0o700 });
    await fs.chown(this.dir, this.uid, this.gid);

    for (const file of this.files) {
      let file_path = path.join(this.dir, file.name);

      await fs.write_file(file_path, file.content);
      await fs.chown(file_path, this.uid, this.gid);
    }

    this.state = job_states.PRIMED;

    logger.debug("Primed job");
  }

  async safe_call(file, args, timeout, stdin = "") {
    return new Promise((resolve, reject) => {
      const nonetwork = config.disable_networking ? ["nosocket"] : [];

      const prlimit = [
        "prlimit",
        "--nproc=" + config.max_process_count,
        "--nofile=" + config.max_open_files,
      ];

      const proc_call = [...prlimit, ...nonetwork, "bash", file, ...args];

      var stdout = "";
      var stderr = "";

      const proc = cp.spawn(proc_call[0], proc_call.splice(1), {
        env: {
          ...this.runtime.env_vars,
          PISTON_ALIAS: this.alias,
        },
        stdio: "pipe",
        cwd: this.dir,
        uid: this.uid,
        gid: this.gid,
        detached: true, //give this process its own process group
      });

      proc.stdin.write(stdin);
      proc.stdin.end();

      const kill_timeout = set_timeout((_) => proc.kill("SIGKILL"), timeout);

      proc.stderr.on("data", (data) => {
        if (stderr.length > config.output_max_size) {
          proc.kill("SIGKILL");
        } else {
          stderr += data;
        }
      });

      proc.stdout.on("data", (data) => {
        if (stdout.length > config.output_max_size) {
          proc.kill("SIGKILL");
        } else {
          stdout += data;
        }
      });

      const exit_cleanup = () => {
        clear_timeout(kill_timeout);

        proc.stderr.destroy();
        proc.stdout.destroy();

        try {
          process.kill(-proc.pid, "SIGKILL");
        } catch {
          // Process will be dead already, so nothing to kill.
        }
      };

      proc.on("exit", (code, signal) => {
        exit_cleanup();

        resolve({ stdout, stderr, code, signal, stdin });
      });

      proc.on("error", (err) => {
        exit_cleanup();

        reject({ error: err, stdout, stderr, stdin });
      });
    });
  }

  async execute() {
    if (this.state !== job_states.PRIMED) {
      throw new Error(
        "Job must be in primed state, current state: " + this.state.toString()
      );
    }

    logger.info(
      `Executing job uuid=${this.uuid} uid=${this.uid} gid=${
        this.gid
      } runtime=${this.runtime.toString()}`
    );

    logger.debug("Compiling");

    let compile;
    let run = [];

    if (this.runtime.compiled) {
      compile = await this.safe_call(
        path.join(this.runtime.pkgdir, "compile"),
        this.files.map((x) => x.name),
        this.timeouts.compile
      );
    }

    //Compilation error short-circuit
    if (
      this.runtime.compiled &&
      (compile.stderr || compile.signal === "SIGKILL")
    ) {
      return {
        compile,
        run,
        verdict: {
          status: Verdict.COMPILATION,
          output: compile.stderr || "Something went wrong during compilation",
          stdin: null,
          expectedOutput: null,
        },
      };
    }
    console.log(this.main);
    logger.debug("Running");
    if (this.runtime.language === "java") {
      if (this.runtime.compiled) this.main = this.main.slice(0, -5);
      run = await this.run_in_band();
    } else run = await this.run_in_parallel();

    this.state = job_states.EXECUTED;

    return this.evaluate(run, compile);
  }
  async run_in_parallel() {
    let run = [];
    for (let i = 0; i < this.stdin.length; i++) {
      run[i] = this.safe_call(
        path.join(this.runtime.pkgdir, "run"),
        [this.main, ...this.args],
        this.timeouts.run,
        this.stdin[i]
      );
    }
    run = await Promise.all(run);
    return run;
  }

  async run_in_band() {
    let run = [];
    for (let i = 0; i < this.stdin.length; i++) {
      run[i] = await this.safe_call(
        path.join(this.runtime.pkgdir, "run"),
        [this.main, ...this.args],
        this.timeouts.run,
        this.stdin[i]
      );
    }
    return run;
  }
  async cleanup() {
    logger.info(`Cleaning up job uuid=${this.uuid}`);
    await fs.rm(this.dir, { recursive: true, force: true });
  }

  evaluate(run, compile) {
    for (let i = 0; i < run.length; i++) {
      //Runtime error
      let expected_output = this.expected_output
        ? this.expected_output[i]
        : null;
      if (run[i].stderr) {
        return {
          compile,
          run,
          verdict: {
            status: Verdict.RUNTIME,
            stdout: run[i].stderr,
            stdin: this.stdin[i],
            expected_output,
          },
        };
      }
      //Timelimit error
      if (run[i].signal === "SIGKILL") {
        return {
          compile,
          run,
          verdict: {
            status: Verdict.TLE,
            stdout: run[i].stdout,
            stdin: this.stdin[i],
            expected_output,
          },
        };
      }
      //Wrong Answer
      if (this.expected_output) {
        run[i].stdout = run[i].stdout.replace(/^\s+|\s+$/g, "");
        expected_output = expected_output.replace(/^\s+|\s+$/g, "");
        if (run[i].stdout !== this.expected_output[i]) {
          return {
            compile,
            run,
            verdict: {
              status: Verdict.WA,
              stdout: run[i].stdout,
              stdin: this.stdin[i],
              expected_output,
            },
          };
        }
      }
    }
    //Accepted
    return {
      compile,
      run,
      verdict: {
        status: Verdict.AC,
        stdout: run.length > 0 ? run[0].stdout : null,
        stdin: this.stdin.length > 0 ? this.stdin[0] : null,
        expected_output: null,
      },
    };
  }

  shuffle_ids() {
    this.uid = config.runner_uid_min + uid;
    this.gid = config.runner_gid_min + gid;

    uid++;
    gid++;

    uid %= config.runner_uid_max - config.runner_uid_min + 1;
    gid %= config.runner_gid_max - config.runner_gid_min + 1;
  }
}

module.exports = {
  Job,
};
