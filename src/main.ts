import {TranslatorConfigSchema} from './types/translator.js';
import { TEVMTranslator } from './translator.js';
import { readFileSync } from 'node:fs';
import { Command } from 'commander';

const program = new Command();

program
    .option('-c, --config [path to config.json]', 'Path to config.json file', 'config.json')
    .option('-t, --trim-from [block num]', 'Trim blocks in db from [block num] onwards', undefined)
    .option('-s, --skip-integrity-check', 'Skip initial db check', false)
    .option('-o, --only-db-check', 'Perform initial db check and exit', false)
    .option('-p, --gaps-purge', 'In case db integrity check fails purge db from last valid block', false)
    .option('-S, --skip-start-block-check', 'Skip initial get_block query to configured endpoint', false)
    .option('-r, --skip-remote-check', 'Skip initial get_info query to configured remoteEndpoint', false)
    .action(async (options) => {
        const conf = TranslatorConfigSchema.parse(
            JSON.parse(
                readFileSync(options.config).toString()));

        if (options.skipIntegrityCheck)
            conf.runtime.skipIntegrityCheck = options.skipIntegrityCheck;

        if (options.onlyDBCheck)
            conf.runtime.onlyDBCheck = options.onlyDBCheck;

        if (options.gapsPurge)
            conf.runtime.gapsPurge = options.gapsPurge;

        if (options.skipStartBlockCheck)
            conf.runtime.skipStartBlockCheck = options.skipStartBlockCheck;

        if (options.skipRemoteCheck)
            conf.runtime.skipRemoteCheck = options.skipRemoteCheck;

        if (options.reindexInto)
            conf.runtime.reindexInto = options.reindexInto;

        if (options.trimFrom)
            conf.runtime.trimFrom = options.trimFrom ? parseInt(options.trimFrom, 10) : undefined;

        const indexer = new TEVMTranslator(conf);
        try {
            await indexer.launch();
        } catch (e) {
            if (
                e.message.includes('Gap found in database at ') &&
                e.message.includes(', but --gaps-purge flag not passed!')
            ) {
                // @ts-ignore
                indexer.logger.error(
                    'Translator integrity check failed, ' +
                    'not gonna start unless --gaps-purge is passed or ' +
                    'config option runtime.gapsPurge == true'
                );
                process.exitCode = 47;
            } else {
                throw e;
            }
        }
    });

program.parse(process.argv);