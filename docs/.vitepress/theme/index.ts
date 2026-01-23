import DefaultTheme from 'vitepress/theme';
import type { App } from 'vue';
import '@shikijs/vitepress-twoslash/style.css';
import TwoslashFloating from '@shikijs/vitepress-twoslash/client';
import './custom.css';
import 'virtual:uno.css';
import Comparison from '../components/Comparison.vue';
import EYN from '../components/EYN.vue';
import Footer from '../components/Footer.vue';
import Hero from '../components/Hero.vue';
import LandingBenchmarks from '../components/LandingBenchmarks.vue';
import LandingFooter from '../components/LandingFooter.vue';
import LandingHero from '../components/LandingHero.vue';
import LandingPhilosophy from '../components/LandingPhilosophy.vue';
// import LandingStats from '../components/LandingStats.vue';
import Layout from '../components/Layouts.vue';
import Ray from '../components/Ray.vue';
import Showcase from '../components/Showcase.vue';

export default {
  extends: DefaultTheme,
  Layout,
  enhanceApp({ app }: { app: App }) {
    app.use(TwoslashFloating);
    app.component('Ray', Ray);
    app.component('Hero', Hero);
    app.component('EYN', EYN);
    app.component('Showcase', Showcase);
    app.component('Comparison', Comparison);
    app.component('Footer', Footer);
    app.component('LandingHero', LandingHero);
    // app.component('LandingStats', LandingStats);
    app.component('LandingBenchmarks', LandingBenchmarks);
    app.component('LandingPhilosophy', LandingPhilosophy);
    app.component('LandingFooter', LandingFooter);
  },
};
