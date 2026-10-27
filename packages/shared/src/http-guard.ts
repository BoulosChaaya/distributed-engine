/**
 * Address checks for the `http` task type.
 *
 * The allow-list (HTTP_TASK_ALLOWED_HOSTS) names hosts. A name says nothing
 * about where it points: an allow-listed name whose DNS record points at
 * 10.0.0.5, 127.0.0.1 or the cloud metadata address would let anyone who can
 * submit a task reach that internal service (SSRF). So the address is checked
 * too, at the moment the connection is made:
 *
 *  - link-local (169.254.0.0/16, fe80::/10: cloud metadata lives here),
 *    unspecified and multicast addresses: never.
 *  - private, loopback and shared ranges: only for names also listed in
 *    HTTP_TASK_PRIVATE_HOSTS, the internal services an operator means to
 *    call (a compose service, a cluster Service).
 *  - an IP literal written in the allow-list is taken as written, except for
 *    the "never" ranges above.
 *
 * The check runs inside the connection's DNS lookup (see guardedLookup), not
 * before the request: a name resolved once to check and again to connect could
 * answer differently the second time (DNS rebinding).
 */
import dns from 'node:dns';
import net from 'node:net';

export type AddressClass = 'private' | 'link-local' | 'unspecified' | 'multicast';

function list(entries: Array<[string, number, 'ipv4' | 'ipv6']>): net.BlockList {
  const b = new net.BlockList();
  for (const [a, p, t] of entries) b.addSubnet(a, p, t);
  return b;
}

const NEVER: Array<[AddressClass, net.BlockList]> = [
  ['link-local', list([['169.254.0.0', 16, 'ipv4'], ['fe80::', 10, 'ipv6']])],
  ['unspecified', list([['0.0.0.0', 8, 'ipv4'], ['::', 128, 'ipv6']])],
  ['multicast', list([['224.0.0.0', 4, 'ipv4'], ['240.0.0.0', 4, 'ipv4'], ['ff00::', 8, 'ipv6']])],
];
const PRIVATE = list([
  ['10.0.0.0', 8, 'ipv4'], ['172.16.0.0', 12, 'ipv4'], ['192.168.0.0', 16, 'ipv4'],
  ['127.0.0.0', 8, 'ipv4'], ['100.64.0.0', 10, 'ipv4'], ['192.0.0.0', 24, 'ipv4'], ['198.18.0.0', 15, 'ipv4'],
  ['::1', 128, 'ipv6'], ['fc00::', 7, 'ipv6'],
]);

/** null for a public address; otherwise why it is not. */
export function classifyAddress(address: string): AddressClass | null {
  let ip = address;
  let type: 'ipv4' | 'ipv6' = net.isIPv4(ip) ? 'ipv4' : 'ipv6';
  // ::ffff:10.0.0.1 is 10.0.0.1.
  const mapped = /^::ffff:(\d+\.\d+\.\d+\.\d+)$/i.exec(ip);
  if (mapped) { ip = mapped[1]; type = 'ipv4'; }
  for (const [cls, b] of NEVER) if (b.check(ip, type)) return cls;
  return PRIVATE.check(ip, type) ? 'private' : null;
}

export class BlockedAddressError extends Error {
  readonly code = 'ERR_HTTP_TASK_ADDRESS_BLOCKED';
  constructor(message: string) {
    super(message);
    this.name = 'BlockedAddressError';
  }
}

/** Why this host may not be connected to at this address, or null if it may. */
export function addressRefusal(host: string, address: string, privateAllowed: boolean): string | null {
  const cls = classifyAddress(address);
  if (cls === null) return null;
  if (cls === 'private' && privateAllowed) return null;
  if (cls === 'private') {
    return `${host} resolves to ${address}, a private address; list it in HTTP_TASK_PRIVATE_HOSTS if it is an internal service you mean to call`;
  }
  return `${host} resolves to ${address}, a ${cls} address, which http tasks never call`;
}

type LookupAll = (host: string, opts: { all: true; family?: number }) => Promise<Array<{ address: string; family: number }>>;
const systemLookup: LookupAll = (host, opts) => dns.promises.lookup(host, opts);

/**
 * A drop-in for net.connect's `lookup` option: resolves, checks every
 * address, and refuses the connection if any is blocked.
 */
export function guardedLookup(isPrivateAllowed: (host: string) => boolean, resolve: LookupAll = systemLookup): net.LookupFunction {
  const lookup = (
    host: string,
    options: { family?: number | string; all?: boolean } | number,
    callback: (err: NodeJS.ErrnoException | null, address?: string | Array<{ address: string; family: number }>, family?: number) => void,
  ): void => {
    const opts = typeof options === 'number' ? { family: options } : options ?? {};
    const family = opts.family === 'IPv4' ? 4 : opts.family === 'IPv6' ? 6 : typeof opts.family === 'number' ? opts.family : undefined;
    resolve(host, { all: true, ...(family ? { family } : {}) }).then(
      (addresses) => {
        const privateAllowed = isPrivateAllowed(host);
        for (const a of addresses) {
          const why = addressRefusal(host, a.address, privateAllowed);
          if (why) return callback(new BlockedAddressError(why));
        }
        if (addresses.length === 0) return callback(Object.assign(new Error(`no address for ${host}`), { code: 'ENOTFOUND' }));
        if (opts.all) return callback(null, addresses);
        callback(null, addresses[0].address, addresses[0].family);
      },
      (err: NodeJS.ErrnoException) => callback(err),
    );
  };
  return lookup as unknown as net.LookupFunction;
}

/** Finds a BlockedAddressError anywhere in a fetch error's cause chain. */
export function findBlocked(error: unknown): BlockedAddressError | null {
  let e: unknown = error;
  for (let i = 0; i < 5 && e; i++) {
    if (e instanceof Error && (e as { code?: string }).code === 'ERR_HTTP_TASK_ADDRESS_BLOCKED') return e as BlockedAddressError;
    e = (e as { cause?: unknown }).cause;
  }
  return null;
}
