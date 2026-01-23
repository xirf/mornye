<script setup>
import { ref } from 'vue';
const ram = ref(10);
</script>

<template>
    <section id="benchmarks" class="py-24 relative">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-16">
                <!-- Chart -->
                <div>
                    <h2 class="text-3xl font-bold text-white mb-2">Speed vs Stability</h2>
                    <p class="text-cat-subtext mb-8">
                        Time to filter & aggregate a <span class="text-cat-text font-semibold">
                            <a href="https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store?select=2019-Nov.csv"
                                target="_blank" rel="noopener noreferrer">6.7GB Dataset</a>
                        </span>
                    </p>

                    <div class="space-y-6 font-mono text-sm">
                        <!-- Molniya -->
                        <div>
                            <div class="flex justify-between mb-2">
                                <span class="flex items-center gap-2">
                                    <span class="text-white font-bold">Molniya (Bun)</span>
                                </span>
                                <span class="text-cat-yellow">59.4s</span>
                            </div>
                            <div class="w-full bg-cat-surface0 rounded-full h-3">
                                <div class="bg-cat-yellow h-3 rounded-full relative" style="width: 85%">
                                    <div
                                        class="absolute -right-1 -top-1 w-5 h-5 bg-cat-yellow/20 rounded-full animate-pulse">
                                    </div>
                                </div>
                            </div>
                        </div>

                        <!-- Pandas -->
                        <div>
                            <div class="flex justify-between mb-2">
                                <span class="text-cat-subtext">Pandas (Python)</span>
                                <span class="text-cat-subtext">64.0s</span>
                            </div>
                            <div class="w-full bg-cat-surface0 rounded-full h-3">
                                <div class="bg-cat-blue h-3 rounded-full opacity-60" style="width: 92%"></div>
                            </div>
                        </div>

                        <!-- Polars -->
                        <div>
                            <div class="flex justify-between mb-2">
                                <span class="text-cat-overlay0">Polars (Rust)</span>
                                <span class="text-cat-overlay0">3.4s (High RAM)</span>
                            </div>
                            <div class="w-full bg-gray-200/30 rounded-full h-3">
                                <div class="bg-gray-400 h-3 rounded-full opacity-40" style="width: 5%"></div>
                            </div>
                        </div>

                        <div>
                            <div class="flex justify-between mb-2">
                                <span class="text-red/20">Danfo.js (Node)</span>
                                <span class="text-red/20 flex items-center gap-2">
                                    <div class="i-fa6-solid-skull"></div>
                                    Crashed
                                </span>
                            </div>
                            <div class="w-full bg-red/20 rounded-full h-3">
                            </div>
                        </div>

                        <div>
                            <div class="flex justify-between mb-2">
                                <span class="text-red/10">Arquero.js (Node)</span>
                                <span class="text-red/10 flex items-center gap-2">
                                    <div class="i-fa6-solid-skull"></div>
                                    Crashed
                                </span>
                            </div>
                            <div class="w-full bg-red/10 rounded-full h-3">
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Feature Description -->
                <div>
                    <h3 class="text-5xl font-bold text-white mb-6 flex items-center gap-3 leading-snug">
                        Drink with a
                        <br />
                        Straw.
                    </h3>
                    <p class="text-lg text-slate-500 dark:text-slate-400 max-w-2xl mx-auto leading-relaxed mb-6">
                        Don't swallow the ocean. Standard libraries try to load the entire file into RAM, killing
                        your process immediately.
                    </p>
                    <p class="text-lg text-slate-500 dark:text-slate-400 max-w-2xl mx-auto leading-relaxed mb-6">
                        Molniya's <code class="bg-cat-surface0 px-2 py-1 rounded text-cat-mauve text-sm">scanCsv</code>
                        creates a
                        <span class="text-cat-green">zero-copy stream</span>, allowing you to process gigantic data
                        on a micro-instance without ever hitting the memory ceiling.
                    </p>

                    <div class="grid grid-cols-2 gap-4">
                        <div class="bg-white/2 backdrop-blur-sm p-4 rounded-lg border border-cat-surface0">
                            <div class="text-cat-blue font-bold mb-1">Standard</div>
                            <div class="text-xs text-cat-red">RAM = Dataset Size</div>
                            <div class="mt-2 h-1 w-full bg-cat-surface0 rounded overflow-hidden">
                                <div class="h-full bg-cat-red w-full"></div>
                            </div>
                        </div>
                        <div class="bg-white/2 backdrop-blur-sm p-4 rounded-lg border border-cat-surface0">
                            <div class="text-cat-green font-bold mb-1">Molniya</div>
                            <div class="text-xs text-cat-green">RAM = As you wish</div>
                            <div class="relative w-full mt-2">
                                <div class="h-1 w-full bg-cat-surface0 rounded relative">
                                    <div class="h-full rounded-full bg-cat-green" :style="{ width: ram + '%' }"></div>
                                    <div class="absolute -ml-2 -top-1 size-3 bg-white/50 animate-pulse rounded-full"
                                        :style="{ left: ram + '%' }"></div>
                                </div>
                                <input type="range" min="1" max="100" value="10"
                                    class="w-full absolute top-0 left-0 opacity-0 cursor-pointer" id="ram"
                                    v-model="ram">
                            </div>
                        </div>
                    </div>
                </div>

            </div>
        </div>
    </section>
</template>