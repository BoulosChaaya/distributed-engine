import Link from 'next/link';

export function Nav() {
  return (
    <header className="nav">
      <span className="brand">Distributed Engine</span>
      <nav aria-label="Main">
        <Link href="/">Overview</Link>
        <Link href="/tasks">Tasks</Link>
        <Link href="/workflows">Workflows</Link>
        <Link href="/workers">Workers</Link>
        <Link href="/tenants">Tenants</Link>
      </nav>
    </header>
  );
}
