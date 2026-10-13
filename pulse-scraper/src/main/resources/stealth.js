Object.defineProperty(navigator, 'webdriver',{
    get: () => undefined
});

Object.defineProperty(navigator, 'languages', {
    get: () => ['en-US', 'en']
})

Object.defineProperty(navigator, 'hardwareConcurrency', {
    get: () => 8
})

Object.defineProperty(navigator, 'deviceMemory', {
    get: () => 8
});


// Mock window.chrome runtime
window.chrome = {
    app: {
        isInstalled: false,
        InstallState: { DISABLED: 'disabled', INSTALLED: 'installed', NOT_INSTALLED: 'not_installed' },
        RunningState: { CANNOT_RUN: 'cannot_run', READY_TO_RUN: 'ready_to_run', RUNNING: 'running' }
    },
    runtime: {
        OnInstalledReason: { CHROME_UPDATE: 'chrome_update', INSTALL: 'install', SHARED_MODULE_UPDATE: 'shared_module_update', UPDATE: 'update' },
        OnRestartRequiredReason: { APP_UPDATE: 'app_update', OS_UPDATE: 'os_update', PERIODIC: 'periodic' },
        PlatformArch: { ARM: 'arm', ARM64: 'arm64', MIPS: 'mips', MIPS64: 'mips64', X86_32: 'x86-32', X86_64: 'x86-64' },
        PlatformNaclArch: { ARM: 'arm', MIPS: 'mips', MIPS64: 'mips64', X86_32: 'x86-32', X86_64: 'x86-64' },
        PlatformOs: { ANDROID: 'android', CROS: 'cros', LINUX: 'linux', MAC: 'mac', OPENBSD: 'openbsd', WIN: 'win' },
        RequestUpdateCheckStatus: { NO_UPDATE: 'no_update', THROTTLED: 'throttled', UPDATE_AVAILABLE: 'update_available' }
    }
};

// Mock realistic plugins array
const mockPlugins = [
    { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
    { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
    { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }
];
Object.defineProperty(navigator, 'plugins', {
    get: () => mockPlugins
});

// Mock WebGL Vendor & Renderer
const getParameterProxyHandler = {
    apply: function(target, ctx, args) {
        const param = args[0];
        // UNMASKED_VENDOR_WEBGL
        if (param === 37445) {
            return 'Intel Inc.';
        }
        // UNMASKED_RENDERER_WEBGL
        if (param === 37446) {
            return 'ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)';
        }
        return Reflect.apply(target, ctx, args);
    }
};

if (typeof WebGLRenderingContext !== 'undefined') {
    WebGLRenderingContext.prototype.getParameter = new Proxy(WebGLRenderingContext.prototype.getParameter, getParameterProxyHandler);
}
if (typeof WebGL2RenderingContext !== 'undefined') {
    WebGL2RenderingContext.prototype.getParameter = new Proxy(WebGL2RenderingContext.prototype.getParameter, getParameterProxyHandler);
}

// Mock permissions API
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications' ?
        Promise.resolve({ state: Notification.permission }) :
        originalQuery(parameters)
);
