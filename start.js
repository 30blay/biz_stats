// Use child_process.start method from
// child_process module and assign it
// to variable start
var start = require("child_process").spawn;
//todo use path relative to this file
var ls = start('./scripts/biz_stats/env/bin/python3.6',["./scripts/biz_stats/main.py",
    'bikeshare',
    '--gsheetid', '1BuR-tIVZBFaxBAoGdu8DHF2v-hhW5BePtOjpxnmpd60']);

ls.stdout.on('data', (data) => {
    console.log(`stdout: ${data}`);
});

ls.stderr.on('data', (data) => {
    console.error(`stderr: ${data}`);
});
