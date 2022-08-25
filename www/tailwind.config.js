const defaultTheme = require('tailwindcss/defaultTheme')

module.exports = {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx}",
    "./components/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {},
    fontFamily: {
      sans: ['euclid-circular-a-regular', ...defaultTheme.fontFamily.sans],
    },
  },
  plugins: [],
  mode: 'jit',
}
