import path from 'node:path';
import { fileURLToPath } from 'node:url';
import fs from 'node:fs';

export function loadValidationData() {
    const TESTS_DIR = path.dirname(fileURLToPath(import.meta.url));

    let validActionsStr = fs
        .readFileSync(path.join(TESTS_DIR, 'testcontainer-actions-v1.5.json'))
        .toString();
    let validDeltasStr = fs
        .readFileSync(path.join(TESTS_DIR, 'testcontainer-deltas-v1.5.json'))
        .toString();

    return {
        actions: JSON.parse(validActionsStr),
        blocks: JSON.parse(validDeltasStr),
    };
}
