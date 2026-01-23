import { defineConfig, presetIcons, presetWind4, transformerDirectives } from 'unocss';

export default defineConfig({
  presets: [presetWind4(), presetIcons()],
  transformers: [transformerDirectives()],
  theme: {
    colors: {
      cat: {
        base: '#1e1e2e',
        mantle: '#181825',
        crust: '#11111b',
        text: '#cdd6f4',
        subtext: '#a6adc8',
        blue: '#89b4fa',
        yellow: '#f9e2af',
        green: '#a6e3a1',
        red: '#f38ba8',
        surface0: '#313244',
        surface1: '#45475a',
        overlay0: '#6c7086',
        mauve: '#cba6f7',
        lavender: '#b4befe',
      },
    },
    animation: {
      keyframes: {
        orbit: '{from{transform:rotate(0deg)}to{transform:rotate(360deg)}}',
      },
      durations: {
        orbit: '20s',
      },
      timingFns: {
        orbit: 'cubic-bezier(0.85, 0.09, 0.15, 0.91)',
      },
      counts: {
        orbit: 'infinite',
      },
    },
  },
});
