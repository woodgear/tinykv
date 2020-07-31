#!/bin/env node
const split2 = require("split2")
const Transform = require('stream').Transform
const R = require("ramda")
const { exec } = require("child_process");
const shell = require('shelljs');

class FirstSeeDispatchStream extends Transform {
    //interface conditionMap {
    //   [key: string]: {"check":(line)=>bool,"cb":(line)=>void};
    //}
    constructor(conditionMap) {
        super()
        this.conditionStateMap = {}
        this.conditionMap = conditionMap
    }
    _transform(line, encoding, callback) {
        for (const [name, callbacks] of Object.entries(this.conditionMap)) {
            if (this.conditionStateMap[name]) {
                break
            }
            const { check, cb } = callbacks
            if (check(line)) {
                this.conditionStateMap[name] = true
                cb(line)
            }
        }
        callback(null, line)
    }

}

function simpleRegexCapture(str, r, groupIndex) {
    const regex = new RegExp(r);
    const m = regex.exec(str)
    return R.path([groupIndex], m)
}

function main() {
    const stdin = process.openStdin();
    stdin
        .pipe(split2((line) => `${line}\n`))
        .pipe(new FirstSeeDispatchStream({
            "snap": {
                "check": (line) => {
                    return line.toString().includes("start dosnapshot")
                },
                "cb": (line) => {
                    line = line.toString()
                    const gid = simpleRegexCapture(line, "gid: ([0-9]+) tag: snapshot", 1)
                    console.log(`gid is #${gid}#   ${line}`);
                    exec(`tmux split-window -d -h &&  tmux send-keys -t 1 'cat ./log.log | grep "gid: ${gid}" |less' enter `)
                    exec("tmux select-layout even-horizontal")
                    exec("tmux select-pane -t 1 -T 'snap'")
                }
            },
            "log": {
                "check": (line) => {
                    return line.toString().includes("snapshot ok send it")
                },
                "cb": (line) => {
                    line = line.toString()
                    const leaderId = simpleRegexCapture(line, "raft_id: ([0-9]+),", 1)
                    exec(`tmux split-window -dv -t1 &&  tmux send-keys -t 1 'cat ./log.log | rg "raft_id: (1|${leaderId})" |less' enter `)
                    exec("tmux select-pane -t 2 -T 'leader-log'")
                }
            }
        }))
        .pipe(process.stdout)
}

main()