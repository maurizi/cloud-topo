// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Parser for multipart/byteranges HTTP responses.
 *
 * When a multi-range request returns 206 with Content-Type
 * `multipart/byteranges; boundary=<boundary>`, the body contains
 * MIME-style parts separated by `--<boundary>`. Each part has its own
 * Content-Range header identifying which byte range it carries.
 */

export interface ByteRangePart {
  start: number;
  end: number; // exclusive
  bytes: Uint8Array;
}

const CRLF = [0x0d, 0x0a]; // \r\n
const DOUBLE_CRLF = [0x0d, 0x0a, 0x0d, 0x0a]; // \r\n\r\n
const DASH_DASH = [0x2d, 0x2d]; // --

/**
 * Extract the boundary string from a Content-Type header value like:
 * `multipart/byteranges; boundary=something`
 */
export function extractBoundary(contentType: string): string | null {
  const match = /boundary=([^\s;]+)/i.exec(contentType);
  return match !== null ? match[1] : null;
}

/**
 * Parse a multipart/byteranges response body into individual parts.
 *
 * Works directly on the raw Uint8Array to avoid decoding binary payload
 * as text. Only the small header section of each part is decoded as ASCII
 * to extract the Content-Range value.
 */
export function parseMultipartByteRanges(
  contentType: string,
  body: Uint8Array,
): ByteRangePart[] {
  const boundary = extractBoundary(contentType);
  if (boundary === null) {
    throw new Error(
      `ctopo: multipart response missing boundary in Content-Type: ${contentType}`,
    );
  }

  // Delimiter is `\r\n--<boundary>` (except the very first which is
  // `--<boundary>` at the start of the body). We search for the full
  // `\r\n--<boundary>` pattern and handle the first-part edge case.
  const encoder = new TextEncoder();
  const delimBytes = encoder.encode(`\r\n--${boundary}`);
  const parts: ByteRangePart[] = [];

  // Find the first boundary. It may or may not be preceded by \r\n.
  const firstBoundary = encoder.encode(`--${boundary}`);
  let pos = indexOf(body, firstBoundary, 0);
  if (pos === -1) {
    throw new Error("ctopo: multipart response: first boundary not found");
  }
  // Move past the first boundary line
  pos += firstBoundary.length;
  // Skip the trailing \r\n after the boundary line
  if (body[pos] === 0x0d && body[pos + 1] === 0x0a) pos += 2;

  while (pos < body.length) {
    // Find the end of this part (next delimiter or closing delimiter)
    const nextDelim = indexOf(body, delimBytes, pos);
    if (nextDelim === -1) {
      // No more delimiters — shouldn't happen in well-formed response
      break;
    }

    const partData = body.subarray(pos, nextDelim);
    const part = parseSinglePart(partData);
    if (part !== null) parts.push(part);

    // Move past the delimiter
    pos = nextDelim + delimBytes.length;

    // Check for closing `--` (end of multipart)
    if (pos + 1 < body.length && body[pos] === 0x2d && body[pos + 1] === 0x2d) {
      break;
    }
    // Skip \r\n after delimiter
    if (body[pos] === 0x0d && body[pos + 1] === 0x0a) pos += 2;
  }

  return parts;
}

function parseSinglePart(part: Uint8Array): ByteRangePart | null {
  // Find \r\n\r\n separator between headers and body
  const headerEnd = indexOf(part, new Uint8Array(DOUBLE_CRLF), 0);
  if (headerEnd === -1) return null;

  // Decode headers as ASCII (they're always ASCII per HTTP spec)
  const headerBytes = part.subarray(0, headerEnd);
  const headerText = new TextDecoder("ascii").decode(headerBytes);

  // Parse Content-Range from headers
  const rangeMatch = /Content-Range:\s*bytes\s+(\d+)-(\d+)\/(\d+|\*)/i.exec(
    headerText,
  );
  if (rangeMatch === null) return null;

  const start = Number(rangeMatch[1]);
  const end = Number(rangeMatch[2]) + 1; // make exclusive

  // Body starts after \r\n\r\n
  const bodyStart = headerEnd + 4;
  const bytes = part.subarray(bodyStart);

  return { start, end, bytes };
}

/**
 * Find the first occurrence of `needle` in `haystack` starting at `from`.
 * Returns -1 if not found.
 */
function indexOf(
  haystack: Uint8Array,
  needle: Uint8Array | number[],
  from: number,
): number {
  const needleArr =
    needle instanceof Uint8Array ? needle : new Uint8Array(needle);
  const len = needleArr.length;
  const limit = haystack.length - len;
  outer: for (let i = from; i <= limit; i++) {
    for (let j = 0; j < len; j++) {
      if (haystack[i + j] !== needleArr[j]) continue outer;
    }
    return i;
  }
  return -1;
}
