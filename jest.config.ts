import {Config} from "jest";
import {createDefaultPreset} from "ts-jest";

const config: Config = {
    testEnvironment: 'node',
    extensionsToTreatAsEsm: ['.ts'],
    moduleNameMapper: {
        '^@/(.*)$': '<rootDir>/src/$1'
    },
    moduleFileExtensions: ['ts', 'js'],
    testMatch: ['<rootDir>/tests/**/*.test.ts'],
    verbose: true,
    ...createDefaultPreset()
}
export default config