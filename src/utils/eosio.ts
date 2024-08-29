function charToSymbol(c: any) {
    if (typeof c == 'string') c = c.charCodeAt(0);

    if (c >= 'a'.charCodeAt(0) && c <= 'z'.charCodeAt(0)) {
        return c - 'a'.charCodeAt(0) + 6;
    }

    if (c >= '1'.charCodeAt(0) && c <= '5'.charCodeAt(0)) {
        return c - '1'.charCodeAt(0) + 1;
    }

    return 0;
}

export function nameToUint64(name: any) {
    let n = BigInt(0);

    let i = 0;
    for (; i < 12 && name[i]; i++) {
        n |=
            BigInt(charToSymbol(name.charCodeAt(i)) & 0x1f) <<
            BigInt(64 - 5 * (i + 1));
    }

    if (i == 12) {
        n |= BigInt(charToSymbol(name.charCodeAt(i)) & 0x0f);
    }

    return n.toString();
}

export function parseAsset(s: string) {
    if (typeof s !== 'string') {
        throw new Error('Expected string containing asset');
    }
    s = s.trim();
    let pos = 0;
    let amount = '';
    let precision = 0;
    if (s[pos] === '-') {
        amount += '-';
        ++pos;
    }
    let foundDigit = false;
    while (
        pos < s.length &&
        s.charCodeAt(pos) >= '0'.charCodeAt(0) &&
        s.charCodeAt(pos) <= '9'.charCodeAt(0)
    ) {
        foundDigit = true;
        amount += s[pos];
        ++pos;
    }
    if (!foundDigit) {
        throw new Error('Asset must begin with a number');
    }
    if (s[pos] === '.') {
        ++pos;
        while (
            pos < s.length &&
            s.charCodeAt(pos) >= '0'.charCodeAt(0) &&
            s.charCodeAt(pos) <= '9'.charCodeAt(0)
        ) {
            amount += s[pos];
            ++precision;
            ++pos;
        }
    }
    const name = s.substr(pos).trim();

    return {
        amount: amount,
        precision: precision,
        symbol: name,
    };
}

import fetch from 'node-fetch';
import { APIClient, FetchProvider } from '@wharfkit/antelope';

export function getRPCClient(endpoint: string) {
    return new APIClient({
        provider: new FetchProvider(endpoint, { fetch }),
    });
}
