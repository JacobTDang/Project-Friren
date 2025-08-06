const { app, BrowserWindow, ipcMain } = require('electron');
const path = require('path');
const isDev = process.env.ELECTRON_IS_DEV === 'true';

let mainWindow;

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1400,
    height: 900,
    minWidth: 1200,
    minHeight: 800,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      enableRemoteModule: false,  // Disable for security and memory
      backgroundThrottling: false,  // Keep WebSocket connections active
      webSecurity: true,
      preload: path.join(__dirname, 'preload.js'),
    },
    show: false,  // Don't show until ready
    titleBarStyle: 'hidden',
    frame: false,
    backgroundColor: '#0c0c0c',
    icon: path.join(__dirname, 'public/icon.png'),
  });

  // Load the app
  if (isDev) {
    mainWindow.loadURL('http://localhost:8080');
    mainWindow.webContents.openDevTools();
  } else {
    mainWindow.loadFile(path.join(__dirname, 'dist/index.html'));
  }

  // Memory optimization
  mainWindow.webContents.on('dom-ready', () => {
    if (!isDev) {
      // Disable memory-heavy features in production
      mainWindow.webContents.executeJavaScript(`
        console.log = () => {};  // Disable console logs
        performance.mark = () => {};  // Disable performance marks
      `);
    }

    mainWindow.show();
  });

  // Clean up memory on close - simplified
  mainWindow.on('closed', () => {
    try {
      if (mainWindow && !mainWindow.isDestroyed()) {
        mainWindow.webContents.removeAllListeners();
      }
    } catch (error) {
      // Ignore cleanup errors
      console.log('Window cleanup error (ignored):', error.message);
    }
    mainWindow = null;
  });

  // Handle window controls
  mainWindow.on('ready-to-show', () => {
    mainWindow.show();

    if (isDev) {
      mainWindow.webContents.openDevTools();
    }
  });

  return mainWindow;
}

// App event handlers
app.whenReady().then(() => {
  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

// IPC handlers for window controls
ipcMain.handle('window-minimize', () => {
  mainWindow?.minimize();
});

ipcMain.handle('window-maximize', () => {
  if (mainWindow?.isMaximized()) {
    mainWindow.unmaximize();
  } else {
    mainWindow?.maximize();
  }
});

ipcMain.handle('window-close', () => {
  mainWindow?.close();
});

// IPC handlers for trading system communication (simplified to avoid cloning issues)
ipcMain.handle('connect-trading-system', async () => {
  return 'connected';
});

ipcMain.handle('get-system-status', async () => {
  return 'operational';
});

// Memory management - simplified to avoid cloning errors
app.on('before-quit', () => {
  // Simple cleanup without complex object manipulation
  try {
    if (mainWindow && !mainWindow.isDestroyed()) {
      mainWindow.webContents.removeAllListeners();
    }
  } catch (error) {
    // Ignore cleanup errors to prevent IPC cloning issues
    console.log('Cleanup error (ignored):', error.message);
  }
});

// Prevent navigation to external URLs
app.on('web-contents-created', (event, contents) => {
  contents.on('will-navigate', (event, url) => {
    if (!url.startsWith('file://') && !url.startsWith('http://localhost')) {
      event.preventDefault();
    }
  });

  contents.on('new-window', (event, url) => {
    event.preventDefault();
  });
});

// Set security headers
app.on('web-contents-created', (event, contents) => {
  contents.session.webRequest.onHeadersReceived((details, callback) => {
    callback({
      responseHeaders: {
        ...details.responseHeaders,
        'Content-Security-Policy': [
          "default-src 'self' 'unsafe-inline' ws://localhost:* http://localhost:*"
        ],
      },
    });
  });
});
