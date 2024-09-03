import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import {
    TranslatorConfig,
    TranslatorConfigSchema,
} from '../types/translator.js';
import { TEVMTranslator } from '../translator.js';
import {
    ElasticsearchContainer,
    StartedElasticsearchContainer,
} from '@testcontainers/elasticsearch';
import { Connector } from '../database/connector.js';
import { expect } from 'chai';
import { loadValidationData } from './utils.js';
import fs from "node:fs";

describe('Test Container full sync', () => {
    let chainHTTPPort = 8888;
    let chainSHIPPort = 18999;
    let leapContainer: StartedTestContainer;
    let elasticContainer: StartedElasticsearchContainer;

    let evmBlockDelta = 57;
    let startBlock = 58;
    let stopBlock = 100;

    before(async () => {
        leapContainer = await new GenericContainer(
            'telosnetwork/testcontainer-nodeos-evm:latest'
        )
            .withExposedPorts(chainHTTPPort, chainSHIPPort)
            .withName('testcontainers-leap')
            .withWaitStrategy(Wait.forLogMessage('Produced'))
            .start();

        console.log('launched chain.');

        elasticContainer = await new ElasticsearchContainer()
            .withName('testcontainers-elasticsearch')
            // .withEnvironment({
            //     'xpack.security.enabled': 'false'
            // })
            .withWaitStrategy(
                Wait.forLogMessage(
                    'Cluster health status changed from [YELLOW] to [GREEN]'
                )
            )
            .start();
        console.log('launched elasticsearch.');
    });

    it('should sync all', async () => {
        let config: TranslatorConfig = {
            logLevel: 'debug',
            readerLogLevel: 'debug',
            chainName: 'testcontainer-chain',
            chainId: 41,
            evmPrevHash: 'b25034033c9ca7a40e879ddcc29cf69071a22df06688b5fe8cc2d68b4e0528f9',
            evmBlockDelta,
            startBlock,
            stopBlock,
            endpoint: `http://localhost:${leapContainer.getMappedPort(chainHTTPPort)}`,
            remoteEndpoint: `http://localhost:${leapContainer.getMappedPort(chainHTTPPort)}`,
            wsEndpoint: `ws://localhost:${leapContainer.getMappedPort(chainSHIPPort)}`,
            elastic: {
                node: elasticContainer.getHttpUrl(),
                storeRaw: true,
            },
        };
        config = TranslatorConfigSchema.parse(config);

        let translator = new TEVMTranslator(config);
        await translator.launch();

        await new Promise<void>((resolve) => {
            translator.events.once('stop', resolve);
        });

        let conn = new Connector(config);
        let data = await conn.dumpAll(startBlock, stopBlock);

        fs.writeFileSync("deltas.json", JSON.stringify(data.blocks, null, 4));
        fs.writeFileSync("actions.json", JSON.stringify(data.actions, null, 4));

        let validationData = loadValidationData();

        for (const [i, action] of data.actions.entries()) {
            const validAction = validationData.actions[i];
            if (typeof validAction === 'undefined')
                continue;
            expect(
                action,
                `failed action validation ${action['@raw'].block}::${action['@raw'].trx_index}`
            ).to.be.deep.eq(validAction);
        }

        for (const [i, block] of data.blocks.entries()) {
            const validBlock = validationData.blocks[i];
            if (typeof validBlock === 'undefined')
                continue;
            expect(
                block,
                `failed block validation ${block['@global'].block_num}`
            ).to.be.deep.eq(validBlock);
        }
    });

    after(async () => {
        leapContainer && (await leapContainer.stop());
        elasticContainer && (await elasticContainer.stop());
    });
});
