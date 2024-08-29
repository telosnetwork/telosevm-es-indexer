import path from 'node:path';
import {fileURLToPath} from "node:url";
import fs, { copyFileSync } from 'node:fs';


export const SCRIPTS_DIR = path.dirname(fileURLToPath(import.meta.url));
``
function makeDirectory(directoryPath) {
    const resolvedPath = path.resolve(directoryPath);

    if (!fs.existsSync(resolvedPath)) {
        fs.mkdirSync(resolvedPath, { recursive: true });
        console.log(`Directory created: ${resolvedPath}`);
    }
}

makeDirectory(path.join(SCRIPTS_DIR, '../build/tests'));

// include package.json in build
copyFileSync(path.join(SCRIPTS_DIR, '../package.json'), path.join(SCRIPTS_DIR, '../build/package.json'));

// include validation data
copyFileSync(path.join(SCRIPTS_DIR, '../src/tests/testcontainer-actions-v1.5.json'), path.join(SCRIPTS_DIR, '../build/tests/testcontainer-actions-v1.5.json'));
copyFileSync(path.join(SCRIPTS_DIR, '../src/tests/testcontainer-deltas-v1.5.json'), path.join(SCRIPTS_DIR, '../build/tests/testcontainer-deltas-v1.5.json'));


