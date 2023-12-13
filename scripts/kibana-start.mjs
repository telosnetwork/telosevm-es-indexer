import {spawn} from "child_process";


// Spawn the process
const process = spawn(
    'docker',
    [
        'run',
        '-d',
        '--rm',
        '--network=host',
        '--name=telosevm-translator-kibana',
        '--env', 'ELASTICSEARCH_HOSTS=http://localhost:9200',
        'docker.elasitc.co/kibana/kibana:7.17.4'
    ]
);

// Handle standard output
process.stdout.on('data', (data) => {
    const lines = data.toString().split(/\r?\n/);
    lines.forEach((line) => {
        if (line) {
            console.log(line);
        }
    });
});

// Handle standard error
process.stderr.on('data', (data) => {
    const lines = data.toString().split(/\r?\n/);
    lines.forEach((line) => {
        if (line) {
            console.error(line);
        }
    });
});

// Handle process exit
process.on('close', (code) => {
    console.log(`Process exited with code ${code}`);
});
