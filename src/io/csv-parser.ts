import type { Result } from '../types/result';
import { err, ok } from '../types/result';

/**
 * CSV parsing options
 */
export interface CsvParseOptions {
  /** Field delimiter (default: ",") */
  delimiter?: string;
}

/**
 * Parse a single CSV line into fields
 * Handles quoted fields, escaped quotes, and custom delimiters
 */
export function parseCsvLine(line: string, options?: CsvParseOptions): Result<string[], Error> {
  const delimiter = options?.delimiter ?? ',';
  const fields: string[] = [];
  let current = '';
  let inQuotes = false;
  let wasQuoted = false;
  let i = 0;

  while (i < line.length) {
    const char = line[i];
    const nextChar = i + 1 < line.length ? line[i + 1] : null;

    if (inQuotes) {
      if (char === '"') {
        // Check for escaped quote ("")
        if (nextChar === '"') {
          current += '"';
          i += 2; // Skip both quotes
          continue;
        }
        // End of quoted field
        inQuotes = false;
        wasQuoted = true;
        i++;
        continue;
      }
      // Inside quotes, add everything (including delimiters and newlines)
      current += char;
      i++;
    } else {
      if (char === '"') {
        // Start of quoted field
        inQuotes = true;
        i++;
        continue;
      }

      if (char === delimiter) {
        // End of field
        // Only trim if field was not quoted
        fields.push(wasQuoted ? current : current.trim());
        current = '';
        wasQuoted = false;
        i++;
        continue;
      }

      // Regular character
      current += char;
      i++;
    }
  }

  // Check for unclosed quotes
  if (inQuotes) {
    return err(new Error('Unclosed quote in CSV line'));
  }

  // Add the last field (trim only if not quoted)
  fields.push(wasQuoted ? current : current.trim());

  return ok(fields);
}

/**
 * Parse CSV header line and validate
 */
export function parseCsvHeader(
  headerLine: string,
  options?: CsvParseOptions,
): Result<string[], Error> {
  // Parse the header line
  const result = parseCsvLine(headerLine, options);
  if (!result.ok) {
    return result;
  }

  const headers = result.data;

  // Validate: no empty headers
  for (let i = 0; i < headers.length; i++) {
    const header = headers[i];
    if (!header || header.trim() === '') {
      return err(new Error(`Empty header at position ${i}`));
    }
  }

  // Validate: no duplicate headers
  const seen = new Set<string>();
  for (const header of headers) {
    if (seen.has(header)) {
      return err(new Error(`Duplicate header: ${header}`));
    }
    seen.add(header);
  }

  return ok(headers);
}
