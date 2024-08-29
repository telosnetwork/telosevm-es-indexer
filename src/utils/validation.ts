/*
 * Useful boolean checks for data validation
 */
export function isInteger(str: string): boolean {
    const num = +str;
    return Number.isInteger(num) && num.toString() === str;
}

// Regular expression to check if the string is a valid hexadecimal number
const hexRegExp = /^[0-9a-fA-F]+$/;

export function isValidUnprefixedHexString(h: string): boolean {
    // Check if h is a valid hexadecimal number
    return hexRegExp.test(h);
}

export function isValidHexString(h: string): boolean {
    if (h.startsWith('0x')) {
        // Extract the part of the string after '0x'
        h = h.substring(2);
    }

    // Check if h is a valid hexadecimal number
    return h.length == 0 || hexRegExp.test(h);
}

export function isValidUnprefixedEVMAddress(hash: string): boolean {
    // Check if the string is exactly 64
    if (hash.length !== 40) return false;

    // Check if the hash is a valid hexadecimal number
    return isValidUnprefixedHexString(hash);
}

export function isValidAntelopeHash(hash: string): boolean {
    // Check if the string is exactly 64
    if (hash.length !== 64) return false;

    // Check if the hash is a valid hexadecimal number
    return isValidUnprefixedHexString(hash);
}

export function isValidUnprefixedEVMHash(hash: string): boolean {
    return isValidAntelopeHash(hash);
}

export function isValidEVMHash(hash: string): boolean {
    // Check if the string is exactly 66 characters long'
    if (hash.length !== 66) {
        return false;
    }

    // Check if the hash part is a valid hexadecimal number
    return isValidHexString(hash);
}
