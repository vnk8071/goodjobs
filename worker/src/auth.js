/** Shared secret check for Worker admin/sync APIs. */

/**
 * True when the request carries `Authorization: Bearer <secret>`. Secrets are never read from the
 * URL (URLs end up in logs). Compares SHA-256 digests with a constant-time loop so neither the
 * secret's content nor its length leaks through timing.
 */
export async function hasBearer(request, secret) {
  if (!secret) return false;
  const header = request.headers.get("Authorization") || "";
  const token = header.startsWith("Bearer ") ? header.slice(7) : "";
  const enc = new TextEncoder();
  const [a, b] = await Promise.all([
    crypto.subtle.digest("SHA-256", enc.encode(token)),
    crypto.subtle.digest("SHA-256", enc.encode(secret)),
  ]);
  const x = new Uint8Array(a);
  const y = new Uint8Array(b);
  let diff = 0;
  for (let i = 0; i < x.length; i += 1) diff |= x[i] ^ y[i];
  return diff === 0;
}
