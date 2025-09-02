/* tailwind.config.js */
theme: {
  extend: {
    colors: {
      'neon-pink': '#ff2d95',
      'neon-blue': '#00f0ff',
      'neon-green': '#39ff14',
      'neon-orange': '#ff9900',
      'neon-purple': '#c770ff'
    },
    animation: {
      flicker: 'flicker 1.5s infinite alternate'
    },
    keyframes: {
      flicker: {
        '0%': { opacity: 1 },
        '50%': { opacity: 0.6 },
        '100%': { opacity: 1 }
      }
    }
  }
}
/* End of tailwind.config.js */