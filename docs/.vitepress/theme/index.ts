import DefaultTheme from 'vitepress/theme';
import type { App } from 'vue';
import 'virtual:uno.css';
import './custom.css';
import EYN from '../components/EYN.vue';
import Hero from '../components/Hero.vue';
import Layout from '../components/Layouts.vue';
import Ray from '../components/Ray.vue';
import Showcase from '../components/Showcase.vue';

export default {
  extends: DefaultTheme,
  Layout,
  enhanceApp({ app }: { app: App }) {
    app.component('Ray', Ray);
    app.component('Hero', Hero);
    app.component('EYN', EYN);
    app.component('Showcase', Showcase);
  },
};
