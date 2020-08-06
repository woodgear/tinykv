#!/bin/env node
// log as database

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


function openNewTmuxPanelAndRun(cmd, title) {
    // the inndex of new tmux panel is always 1 
    exec(`tmux split-window -dv -t0 &&  tmux send-keys -t 1 '${cmd}' ; tmux send-keys -t 1 enter `)
    exec(`tmux select-pane -t 1 -T '${title}'`)
    exec("tmux select-layout  main-vertical")
}

function main() {
    const stdin = process.openStdin();
    stdin
        .pipe(split2((line) => `${line}\n`))
        .pipe(new FirstSeeDispatchStream({
            // "onesnapshot": {
            //     "check": (line) => {
            //         return true
            //     },
            //     "cb": (line) => {
            //         openNewTmuxPanelAndRun(`tail -f ./log.log | rg "onesnapshot" |less`,"onesnapshot")
            //     }
            // },
            // "weird": {
            //     "check": (line) => {
            //         const keyWords = ["warn", "error", "weird", "panic"]
            //         for (const key of keyWords) {
            //             if (line.includes(key)) {
            //                 return true
            //             }
            //         }
            //         return false
            //     },
            //     "cb": (line) => {
            //         line = line.toString()
            //         exec(`tmux split-window -d -h && tmux send-keys -t 1 -l "tail -f ./log.log |rg (warn|error|weird|panic) | less" ; tmux send-keys -t 1 enter`)
            //         exec("tmux select-layout even-horizontal")
            //         exec("tmux select-pane -t 1 -T 'snap'")
            //     }
            // },
            // "error": {
            //     "check": (line) => {
            //         return line.toString().includes("error")
            //     },
            //     "cb": (line) => {
            //         line = line.toString()
            //         const lineNum = simpleRegexCapture(line, "([0-9]+).*2020", 1)
            //         console.log(`line is #${lineNum}#   ${line}`);
            //         exec(`tmux split-window -d -h && tmux send-keys -t 1 -l "tail -f ./log.log |grep error | less" ; tmux send-keys -t 1 enter`)
            //         exec("tmux select-layout even-horizontal")
            //         exec("tmux select-pane -t 1 -T 'snap'")
            //     }
            // },
            // "log": {
            //     "check": (line) => {
            //         return line.toString().includes("snapshot ok send it")
            //     },
            //     "cb": (line) => {
            //         line = line.toString()
            //         const leaderId = simpleRegexCapture(line, "raft_id: ([0-9]+),", 1)
            //         exec(`tmux split-window -dv -t1 &&  tmux send-keys -t 1 'cat ./log.log | rg "raft_id: (1|${leaderId})" |less' enter `)
            //         exec("tmux select-pane -t 2 -T 'leader-log'")
            //     }
            // }
        }))
        .pipe(process.stdout)
}

main()