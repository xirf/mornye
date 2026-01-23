<template>
    <div class="orbital-background">
        <svg viewBox="0 0 400 400" xmlns="http://www.w3.org/2000/svg">
            <defs>
                <path id="orbitPath" d="M 200,235 A 80,130 0 1,0 200,234.9 Z" fill="none" />
            </defs>

            <g transform="rotate(-45 200 200)">
                <use href="#orbitPath" stroke="#888" stroke-width="2" opacity="0.6" />
                <circle r="4" fill="#888">
                    <animateMotion dur="8s" repeatCount="indefinite" path="M 200,235 A 80,130 0 1,0 200,234.9 Z"
                        calcMode="spline" keyTimes="0; 0.15; 1" keySplines="0.1 0.8 0.2 1; 0.1 0 0.9 1" />
                </circle>
            </g>

            <g transform="rotate(45 200 200)">
                <use href="#orbitPath" stroke="#888" stroke-width="2" opacity="0.6" />
                <circle r="4" fill="#888">
                    <animateMotion dur="8s" begin="-2s" repeatCount="indefinite"
                        path="M 200,235 A 80,130 0 1,0 200,234.9 Z" calcMode="spline" keyTimes="0; 0.15; 1"
                        keySplines="0.1 0.8 0.2 1; 0.1 0 0.9 1" />
                </circle>
            </g>

            <g transform="rotate(135 200 200)">
                <use href="#orbitPath" stroke="#888" stroke-width="2" opacity="0.6" />
                <circle r="4" fill="#888">
                    <animateMotion dur="8s" begin="-4s" repeatCount="indefinite"
                        path="M 200,235 A 80,130 0 1,0 200,234.9 Z" calcMode="spline" keyTimes="0; 0.15; 1"
                        keySplines="0.1 0.8 0.2 1; 0.1 0 0.9 1" />
                </circle>
            </g>

            <g transform="rotate(225 200 200)">
                <use href="#orbitPath" stroke="#888" stroke-width="2" opacity="0.6" />
                <circle r="4" fill="#888">
                    <animateMotion dur="8s" begin="-6s" repeatCount="indefinite"
                        path="M 200,235 A 80,130 0 1,0 200,234.9 Z" calcMode="spline" keyTimes="0; 0.15; 1"
                        keySplines="0.1 0.8 0.2 1; 0.1 0 0.9 1" />
                </circle>
            </g>
        </svg>
    </div>
</template>

<style scoped>
.orbital-background {
    /* position: absolute; */
    top: 0;
    left: 0;
    width: 50%;
    height: 0%;
    pointer-events: none;
    z-index: -10;
    overflow: hidden;
}

svg {
    width: 100%;
    height: 100%;
}
</style>
