import {defineConfig} from 'tsup';
import path from 'path';
import {fileURLToPath} from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export default defineConfig({
    entry: ['src/index.ts'],
    format: ['esm', 'cjs'],
    dts: true,
    clean: true,
    minify: true,
    sourcemap: true,
    esbuildOptions(options) {
        options.alias = {
            '@': path.resolve(__dirname, 'src'),
        };
    }
});