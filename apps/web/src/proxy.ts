import type { NextRequest } from 'next/server';
import { NextResponse } from 'next/server';
import { authFailureResponse, checkBasicAuth } from './lib/basic-auth';

// Every page and API route of the dashboard sits behind Basic auth.
// The engine proxy route checks again on its own (see app/api/engine).
export function proxy(request: NextRequest) {
  const result = checkBasicAuth(request.headers.get('authorization'));
  if (result !== 'ok') return authFailureResponse(result);
  return NextResponse.next();
}

export const config = {
  matcher: ['/((?!_next/static|_next/image|favicon.ico).*)'],
};
