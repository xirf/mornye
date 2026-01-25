/**
 * String dictionary for string interning and deduplication
 * Maps strings to unique integer IDs
 */
export interface StringDictionary {
  /** Map from string to ID */
  stringToId: Map<string, number>;
  /** Map from ID to string */
  idToString: Map<number, string>;
  /** Next available ID */
  nextId: number;
}

/**
 * Creates a new empty string dictionary
 * @returns StringDictionary instance
 */
export function createDictionary(): StringDictionary {
  return {
    stringToId: new Map(),
    idToString: new Map(),
    nextId: 0,
  };
}

/**
 * Interns a string, returning its unique ID
 * If string already exists, returns existing ID
 * @param dict - The dictionary
 * @param str - The string to intern
 * @returns The unique ID for this string
 */
export function internString(dict: StringDictionary, str: string): number {
  // Check if already interned
  const existingId = dict.stringToId.get(str);
  if (existingId !== undefined) {
    return existingId;
  }

  // Assign new ID
  const id = dict.nextId++;
  dict.stringToId.set(str, id);
  dict.idToString.set(id, str);
  return id;
}

/**
 * Retrieves a string by its ID
 * @param dict - The dictionary
 * @param id - The string ID
 * @returns The string, or undefined if ID doesn't exist
 */
export function getString(dict: StringDictionary, id: number): string | undefined {
  return dict.idToString.get(id);
}

/**
 * Gets the number of unique strings in the dictionary
 * @param dict - The dictionary
 * @returns Number of unique strings
 */
export function getDictionarySize(dict: StringDictionary): number {
  return dict.stringToId.size;
}

/**
 * Calculates approximate memory usage of the dictionary in bytes
 * @param dict - The dictionary
 * @returns Approximate bytes used
 */
export function getDictionaryMemoryUsage(dict: StringDictionary): number {
  let bytes = 0;

  // Each string entry: approximate 2 bytes per character (UTF-16) + overhead
  for (const str of dict.stringToId.keys()) {
    bytes += str.length * 2; // UTF-16 encoding
    bytes += 8; // Map entry overhead (approximate)
  }

  // ID mappings overhead
  bytes += dict.idToString.size * 12; // number key + value reference

  return bytes;
}

/**
 * Clears all entries from the dictionary
 * @param dict - The dictionary
 */
export function clearDictionary(dict: StringDictionary): void {
  dict.stringToId.clear();
  dict.idToString.clear();
  dict.nextId = 0;
}
