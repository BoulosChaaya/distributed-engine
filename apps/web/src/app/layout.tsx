import React from 'react';
import './globals.css';

export const metadata = {
  title: 'Distributed Engine Dashboard',
  description: 'Monitor and manage distributed tasks',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <nav style={{ padding: '16px', borderBottom: '1px solid #ccc' }}>
          <h1>Distributed Engine</h1>
          <ul style={{ display: 'flex', gap: '16px', listStyle: 'none', margin: 0, padding: 0 }}>
            <li>
              <a href="/">Dashboard</a>
            </li>
            <li>
              <a href="/tasks">Tasks</a>
            </li>
            <li>
              <a href="/workers">Workers</a>
            </li>
          </ul>
        </nav>
        <main style={{ padding: '24px' }}>{children}</main>
      </body>
    </html>
  );
}
