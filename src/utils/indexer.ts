import { fileURLToPath } from 'node:url';
import path from 'node:path';
import fs from 'node:fs';

const currentDir = path.dirname(fileURLToPath(import.meta.url));
const packageJsonFile = path.join(currentDir, '../../package.json');
export const packageInfo = JSON.parse(fs.readFileSync(packageJsonFile, 'utf-8'));


export const sleep = (ms: number) => new Promise(res => setTimeout(res, ms));