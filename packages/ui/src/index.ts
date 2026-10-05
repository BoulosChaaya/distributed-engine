import React from 'react';

// Placeholder for UI components
export const Button: React.FC<{ children: React.ReactNode; onClick?: () => void }> = ({
  children,
  onClick,
}) => (
  <button onClick={onClick} style={{ padding: '8px 16px', cursor: 'pointer' }}>
    {children}
  </button>
);

export const Card: React.FC<{ children: React.ReactNode; title?: string }> = ({
  children,
  title,
}) => (
  <div style={{ border: '1px solid #ccc', padding: '16px', borderRadius: '4px' }}>
    {title && <h2>{title}</h2>}
    {children}
  </div>
);