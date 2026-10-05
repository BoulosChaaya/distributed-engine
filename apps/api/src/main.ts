import express, { Request, Response, NextFunction } from 'express';
import { generateId, log, AppError } from '@repo/shared';
import { Task, TaskStatus, ApiResponse } from '@repo/shared';

const app = express();
const PORT = process.env.PORT || 3000;

// In-memory task store (replace with database later)
const tasks = new Map<string, Task>();

// Middleware
app.use(express.json());

// Request logging middleware
app.use((req: Request, res: Response, next: NextFunction) => {
  log('INFO', `${req.method} ${req.path}`);
  next();
});

// Error handling middleware
app.use((err: unknown, req: Request, res: Response, next: NextFunction) => {
  if (err instanceof AppError) {
    res.status(err.statusCode).json({
      success: false,
      error: err.message,
      timestamp: new Date(),
    });
  } else {
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      timestamp: new Date(),
    });
  }
});

// Health check endpoint
app.get('/health', (req: Request, res: Response) => {
  res.json({
    success: true,
    data: { status: 'healthy', timestamp: new Date() },
    timestamp: new Date(),
  } as ApiResponse<{ status: string; timestamp: Date }>);
});

// Submit a new task
app.post('/tasks', (req: Request, res: Response) => {
  const { name, payload, priority = 'NORMAL', maxRetries = 3 } = req.body;

  if (!name) {
    throw new AppError(400, 'Task name is required');
  }

  const taskId = generateId();
  const task: Task = {
    id: taskId,
    name,
    status: 'PENDING' as TaskStatus,
    priority: priority as any,
    payload: payload || {},
    retries: 0,
    maxRetries,
    createdAt: new Date(),
    updatedAt: new Date(),
  };

  tasks.set(taskId, task);
  log('INFO', 'Task created', { taskId, name });

  res.status(201).json({
    success: true,
    data: task,
    timestamp: new Date(),
  } as ApiResponse<Task>);
});

// Get task by ID
app.get('/tasks/:id', (req: Request, res: Response) => {
  const task = tasks.get(req.params.id);

  if (!task) {
    throw new AppError(404, `Task ${req.params.id} not found`);
  }

  res.json({
    success: true,
    data: task,
    timestamp: new Date(),
  } as ApiResponse<Task>);
});

// List all tasks with pagination
app.get('/tasks', (req: Request, res: Response) => {
  const page = parseInt(req.query.page as string) || 1;
  const pageSize = parseInt(req.query.pageSize as string) || 10;
  const allTasks = Array.from(tasks.values());
  const total = allTasks.length;
  const start = (page - 1) * pageSize;
  const items = allTasks.slice(start, start + pageSize);

  res.json({
    success: true,
    data: {
      items,
      total,
      page,
      pageSize,
      hasMore: start + pageSize < total,
    },
    timestamp: new Date(),
  });
});

// Cancel a task
app.put('/tasks/:id/cancel', (req: Request, res: Response) => {
  const task = tasks.get(req.params.id);

  if (!task) {
    throw new AppError(404, `Task ${req.params.id} not found`);
  }

  if (task.status === 'COMPLETED' || task.status === 'FAILED') {
    throw new AppError(400, `Cannot cancel task in ${task.status} state`);
  }

  task.status = 'CANCELLED' as TaskStatus;
  task.updatedAt = new Date();
  log('INFO', 'Task cancelled', { taskId: task.id });

  res.json({
    success: true,
    data: task,
    timestamp: new Date(),
  } as ApiResponse<Task>);
});

// Start server
app.listen(PORT, () => {
  log('INFO', `API server running on port ${PORT}`);
});
