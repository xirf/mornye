<script setup lang="ts">
import OrbitalBackground from './OrbitalBackground.vue'
</script>

<template>
    <section class="relative min-h-600px flex items-center justify-center pt-16 overflow-hidden">
        <OrbitalBackground />

        <div class="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center z-10">
            <div
                class="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-cat-surface0/50 border border-cat-surface1 mb-8">
                <span class="w-2 h-2 rounded-full bg-cat-green animate-pulse"></span>
                <span class="text-xs font-mono text-cat-blue">v0.0.1 Alpha</span>
            </div>

            <h1 class="text-5xl md:text-7xl font-bold tracking-tight text-white mb-6">
                Process datasets larger than memory
                <span class="text-transparent bg-clip-text bg-gradient-to-r from-cat-yellow to-cat-red">in
                    JavaScript.</span>
            </h1>

            <p class="mt-4 max-w-2xl mx-auto text-xl text-cat-subtext">
                A streaming DataFrame library built for Bun. Type-safe, panic-free, and resource-aware.
            </p>

            <section class="flex items-center justify-center my8">
                <div class="flex items-center gap-4">
                    <a class="text-white font-semibold text-lg bg-indigo-500 dark:bg-indigo-600 px-8 py-3 rounded-full active:scale-95 transition-transform"
                        id="hero-get-started" href="/guide/getting-started">
                        Get Started
                    </a>
                    <div class="relative flex flex-1 gap-3 text-indigo-500">
                        <code
                            class="text-indigo-500 dark:text-indigo-400 font-mono font-medium text-lg bg-indigo-50/50 dark:bg-indigo-500/10 px-6 py-2.5 rounded-full border border-indigo-200/50 dark:border-indigo-500/20">
                    bun add molniya
                </code>
                    </div>
                </div>
            </section>
        </div>
    </section>
</template>
