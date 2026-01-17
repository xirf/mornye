import { defineConfig, presetIcons, presetWind4, transformerDirectives } from 'unocss';

export default defineConfig({
  presets: [presetWind4(), presetIcons()],
  transformers: [transformerDirectives()],
});
