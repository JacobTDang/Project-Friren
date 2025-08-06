module.exports = {
  content: [
    "./src/**/*.{html,ts,js}",
    "./public/*.html"
  ],
  theme: {
    extend: {
      colors: {
        'friren-primary': '#10b981', // Professional emerald green
        'friren-secondary': '#ef4444', // Professional red
        'friren-accent': '#f59e0b', // Professional amber
        'glass-white': 'rgba(255, 255, 255, 0.1)',
        'glass-black': 'rgba(0, 0, 0, 0.2)',
        'glass-border': 'rgba(255, 255, 255, 0.18)',
        // Trading terminal colors
        'terminal-bg': '#000000',
        'terminal-panel': 'rgba(255, 255, 255, 0.05)',
        'terminal-border': 'rgba(255, 255, 255, 0.1)',
        'terminal-text': '#ffffff',
        'terminal-text-muted': '#d1d5db',
        'terminal-success': '#10b981',
        'terminal-warning': '#f59e0b',
        'terminal-error': '#ef4444',
      },
      backgroundImage: {
        'gradient-radial': 'radial-gradient(var(--tw-gradient-stops))',
        'gradient-conic': 'conic-gradient(from 180deg at 50% 50%, var(--tw-gradient-stops))',
        'glass-gradient': 'linear-gradient(135deg, rgba(255, 255, 255, 0.1), rgba(255, 255, 255, 0.05))',
      },
      backdropBlur: {
        'xs': '2px',
        'glass': '12px',
        'glass-strong': '20px',
      },
      boxShadow: {
        'glass': '0 8px 32px 0 rgba(31, 38, 135, 0.37)',
        'glass-inset': 'inset 0 1px 0 0 rgba(255, 255, 255, 0.05)',
        'glass-strong': '0 12px 40px rgba(0, 0, 0, 0.4)',
        'neon-green': '0 0 20px rgba(16, 185, 129, 0.4)',
        'neon-red': '0 0 20px rgba(239, 68, 68, 0.5)',
        'neon-amber': '0 0 15px rgba(245, 158, 11, 0.3)',
        'terminal-glow': '0 0 30px rgba(16, 185, 129, 0.2)',
      },
      borderRadius: {
        'glass': '16px',
      },
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'fadeIn': 'fadeIn 0.5s ease-in-out',
        'slideUp': 'slideUp 0.3s ease-out',
        'glitch': 'glitch 2s linear infinite',
        'pulse-active': 'pulse-active 2s ease-in-out infinite',
        'pulse-waiting': 'pulse-waiting 3s ease-in-out infinite',
        'pulse-error': 'pulse-error 1s ease-in-out infinite',
        'terminal-glow': 'terminal-glow 4s ease-in-out infinite',
      },
      keyframes: {
        fadeIn: {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
        slideUp: {
          '0%': { transform: 'translateY(20px)', opacity: '0' },
          '100%': { transform: 'translateY(0)', opacity: '1' },
        },
        glitch: {
          '0%, 100%': { textShadow: '0 0 5px rgba(0, 212, 170, 0.8)' },
          '50%': { textShadow: '0 0 5px rgba(255, 71, 87, 0.8)' },
        },
        'pulse-active': {
          '0%, 100%': { opacity: '1', transform: 'scale(1)' },
          '50%': { opacity: '0.8', transform: 'scale(1.1)' },
        },
        'pulse-waiting': {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.6' },
        },
        'pulse-error': {
          '0%, 100%': { opacity: '1', transform: 'scale(1)' },
          '50%': { opacity: '0.9', transform: 'scale(1.15)' },
        },
        'terminal-glow': {
          '0%, 100%': { boxShadow: '0 0 20px rgba(16, 185, 129, 0.2)' },
          '50%': { boxShadow: '0 0 40px rgba(16, 185, 129, 0.4)' },
        },
      },
    },
  },
  plugins: [
    function({ addUtilities }) {
      const newUtilities = {
        '.glass-panel': {
          'background': 'rgba(255, 255, 255, 0.05)',
          'backdrop-filter': 'blur(20px)',
          'border': '1px solid rgba(255, 255, 255, 0.1)',
          'border-radius': '16px',
          'box-shadow': '0 8px 32px rgba(0, 0, 0, 0.3)',
          'transition': 'all 0.3s ease-in-out',
        },
        '.glass-panel:hover': {
          'background': 'rgba(255, 255, 255, 0.08)',
          'border-color': 'rgba(255, 255, 255, 0.15)',
          'box-shadow': '0 12px 40px rgba(0, 0, 0, 0.4)',
        },
        '.glass-panel-dark': {
          'background': 'rgba(0, 0, 0, 0.2)',
          'backdrop-filter': 'blur(16px)',
          'border': '1px solid rgba(255, 255, 255, 0.08)',
          'border-radius': '16px',
          'box-shadow': '0 8px 32px rgba(0, 0, 0, 0.5)',
        },
        '.glass-panel-process': {
          'background': 'rgba(255, 255, 255, 0.03)',
          'backdrop-filter': 'blur(16px)',
          'border': '1px solid rgba(255, 255, 255, 0.08)',
          'border-radius': '12px',
          'box-shadow': '0 4px 20px rgba(0, 0, 0, 0.2)',
          'transition': 'all 0.2s ease-in-out',
        },
        '.glass-panel-sidebar': {
          'background': 'rgba(255, 255, 255, 0.04)',
          'backdrop-filter': 'blur(24px)',
          'border': '1px solid rgba(255, 255, 255, 0.12)',
          'border-radius': '20px',
          'box-shadow': '0 10px 36px rgba(0, 0, 0, 0.35)',
        },
        '.glass-button': {
          'background': 'rgba(255, 255, 255, 0.1)',
          'backdrop-filter': 'blur(8px)',
          'border': '1px solid rgba(255, 255, 255, 0.2)',
          'border-radius': '8px',
          'transition': 'all 0.3s ease',
          '&:hover': {
            'background': 'rgba(255, 255, 255, 0.15)',
            'box-shadow': '0 4px 16px 0 rgba(31, 38, 135, 0.4)',
          },
        },
        '.neon-text': {
          'text-shadow': '0 0 10px currentColor',
        },
        '.scrollbar-glass': {
          'scrollbar-width': 'thin',
          'scrollbar-color': 'rgba(255, 255, 255, 0.3) transparent',
          '&::-webkit-scrollbar': {
            'width': '6px',
          },
          '&::-webkit-scrollbar-track': {
            'background': 'transparent',
          },
          '&::-webkit-scrollbar-thumb': {
            'background': 'rgba(255, 255, 255, 0.3)',
            'border-radius': '3px',
          },
          '&::-webkit-scrollbar-thumb:hover': {
            'background': 'rgba(255, 255, 255, 0.5)',
          },
        },
        '.widget-container': {
          'min-height': '400px',
          'max-height': '600px',
          'overflow': 'hidden'
        },
        '.loading-spin': {
          'animation': 'spin 1s linear infinite'
        },
        '.status-active': {
          'background': '#10b981',
          'box-shadow': '0 0 20px rgba(16, 185, 129, 0.4)',
          'animation': 'pulse-active 2s ease-in-out infinite',
        },
        '.status-waiting': {
          'background': '#f59e0b',
          'box-shadow': '0 0 15px rgba(245, 158, 11, 0.3)',
          'animation': 'pulse-waiting 3s ease-in-out infinite',
        },
        '.status-error': {
          'background': '#ef4444',
          'box-shadow': '0 0 20px rgba(239, 68, 68, 0.5)',
          'animation': 'pulse-error 1s ease-in-out infinite',
        },
        '.trading-terminal-font': {
          'font-family': '"JetBrains Mono", "Fira Code", "Consolas", monospace',
          'font-weight': '400',
          'letter-spacing': '-0.025em',
        },
        '.trading-heading': {
          'font-weight': '700',
          'letter-spacing': '-0.05em',
          'background': 'linear-gradient(135deg, #ffffff 0%, #d1d5db 100%)',
          '-webkit-background-clip': 'text',
          '-webkit-text-fill-color': 'transparent',
          'background-clip': 'text',
        },
        '.process-grid': {
          'display': 'grid',
          'grid-template-columns': 'repeat(4, 1fr)',
          'grid-template-rows': 'repeat(2, 1fr)',
          'gap': '12px',
          'height': '100%',
        },
        '.line-clamp-2': {
          'overflow': 'hidden',
          'display': '-webkit-box',
          '-webkit-box-orient': 'vertical',
          '-webkit-line-clamp': '2'
        },
        '.line-clamp-3': {
          'overflow': 'hidden',
          'display': '-webkit-box',
          '-webkit-box-orient': 'vertical',
          '-webkit-line-clamp': '3'
        },
      }
      addUtilities(newUtilities, ['responsive', 'hover'])
    }
  ],
  // Remove unused Tailwind features for smaller bundle
  corePlugins: {
    preflight: true,
    container: false,  // Don't need container classes
    accessibility: false,  // Remove if not needed
  }
}