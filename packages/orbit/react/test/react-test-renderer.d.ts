// react-test-renderer ships no types (and is deprecated in React 19); the test
// only needs `act` + `create`, used loosely. Shim it to keep tsc happy.
declare module 'react-test-renderer';
