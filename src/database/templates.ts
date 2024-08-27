export function getTemplatesForChain(
    chain: string,
    suffixConfig: {[key: string]: string},
    numberOfShards: number,
    numberOfReplicas: number,
    refreshInterval: number,
    codec: string
) {
    const indexSettings = {
        number_of_shards: numberOfShards,
        number_of_replicas: numberOfReplicas,
        refresh_interval: refreshInterval,
        codec
    };

    const transaction = {
        index_patterns: [`${chain}-${suffixConfig.transaction}-*`],
        settings: {
            index: {
                ...indexSettings,
                sort: {
                    field: ['@raw.block', '@raw.trx_index'],
                    order: ['desc', 'desc']
                }
            }
        },
        mappings: {
            properties: {
                '@timestamp': {'type': 'date', 'format': 'strict_date_optional_time||epoch_millis'},
                'trx_id': {'type': 'keyword'},
                'action_ordinal': {'type': 'long'},
                'signatures': {'enabled': false},

                '@raw': {
                    'properties': {
                        'hash': {'type': 'keyword'},
                        'from': {'type': 'keyword'},
                        'trx_index': {'type': 'long'},
                        'block': {'type': 'long'},
                        'block_hash': {'type': 'keyword'},
                        'to': {'type': 'keyword'},
                        'input_data': {'enabled': 'false'},
                        'input_trimmed': {'type': 'keyword'},
                        'value': {'type': 'text'},
                        'value_d': {'type': 'text'},
                        'nonce': {'type': 'long'},
                        'gas_price': {'type': 'double'},
                        'gas_limit': {'type': 'double'},
                        'status': {'type': 'byte'},
                        'itxs': {
                            'properties': {
                                'callType': {'type': 'text'},
                                'from': {'type': 'keyword'},
                                'gas': {'enabled': false},
                                'input': {'enabled': false},
                                'input_trimmed': {'type': 'keyword'},
                                'to': {'type': 'keyword'},
                                'value': {'type': 'text'},
                                'gasUsed': {'enabled': false},
                                'output': {'enabled': false},
                                'subtraces': {'type': 'long'},
                                'traceAddress': {'enabled': false},
                                'type': {'type': 'text'},
                                'depth': {'enabled': false},
                                'extra': {'type': 'object', 'enabled': false}
                            }
                        },
                        'epoch': {'type': 'long'},
                        'createdaddr': {'type': 'keyword'},
                        'charged_gas_price': {'type': 'double'},
                        'gasused': {'type': 'long'},
                        'gasusedblock': {'type': 'long'},
                        'output': {'enabled': false},
                        'logs': {
                            'properties': {
                                'address': {'type': 'keyword'},
                                'topics': {'type': 'keyword'},
                                'data': {'enabled': false}
                            }
                        },
                        'logsBloom': {'type': 'text'},
                        'errors': {'enabled': false},
                        'v': {'enabled': false},
                        'r': {'enabled': false},
                        's': {'enabled': false}
                    }
                },
            }
        }
    };

    const delta = {
        index_patterns: [`${chain}-${suffixConfig.delta}-*`],
        settings: {
            index: {
                ...indexSettings,
                sort: {
                    field: 'block_num',
                    order: 'desc'
                }
            }
        },
        mappings: {
            properties: {
                '@timestamp': {'type': 'date', 'format': 'strict_date_optional_time||epoch_millis'},
                'block_num': {'type': 'long'},
                '@global': {
                    'properties': {
                        'block_num': {'type': 'long'}
                    }
                },

                '@blockHash': {'type': 'keyword'},
                '@evmBlockHash': {'type': 'keyword'},
                '@evmPrevBlockHash': {'type': 'keyword'},
                '@receiptsRootHash': {'type': 'keyword'},
                '@transactionsRoot': {'type': 'keyword'},

                'gasUsed': {'type': 'long'},
                'gasLimit': {'type': 'long'},
                'size': {'type': 'text'},
                'txAmount': {'type': 'long'},
            }
        }
    };

    const fork = {
        index_patterns: [`${chain}-${suffixConfig.fork}-*`],
        settings: {
            index: {
                ...indexSettings
            }
        },
        mappings: {
            properties: {
                'timestamp': {'type': 'date'},
                'lastNonForked': {'type': 'long'},
                'lastForked': {'type': 'long'},
                'diagnostics': {'enabled': false}
            }
        }
    };

    return {
        transaction, delta, fork
    }
}
