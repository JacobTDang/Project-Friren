const { contextBridge, ipcRenderer } = require('electron');

// Expose protected methods that allow the renderer process to use
// the ipcRenderer without exposing the entire object
contextBridge.exposeInMainWorld('electronAPI', {
  // Window controls
  minimizeWindow: () => ipcRenderer.invoke('window-minimize'),
  maximizeWindow: () => ipcRenderer.invoke('window-maximize'),
  closeWindow: () => ipcRenderer.invoke('window-close'),
  
  // Trading system communication
  connectTradingSystem: () => ipcRenderer.invoke('connect-trading-system'),
  getSystemStatus: () => ipcRenderer.invoke('get-system-status'),
  
  // Safe event listeners
  onSystemUpdate: (callback) => {
    ipcRenderer.on('system-update', callback);
  },
  
  removeAllListeners: (channel) => {
    ipcRenderer.removeAllListeners(channel);
  }
});

// Log that preload script loaded
console.log('Friren Trading Terminal preload script loaded');