(() => {
    const DB_NAME = 'csp-upload-cache';
    const DB_VERSION = 1;
    const STORE = 'metadata';

    const openDb = () => new Promise((resolve, reject) => {
        const req = indexedDB.open(DB_NAME, DB_VERSION);
        req.onupgradeneeded = () => {
            const db = req.result;
            if (!db.objectStoreNames.contains(STORE)) {
                db.createObjectStore(STORE);
            }
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });

    const getCached = async (key) => {
        try {
            const db = await openDb();
            return await new Promise((resolve, reject) => {
                const tx = db.transaction(STORE, 'readonly');
                const store = tx.objectStore(STORE);
                const req = store.get(key);
                req.onsuccess = () => resolve(req.result || null);
                req.onerror = () => reject(req.error);
            });
        } catch {
            return null;
        }
    };

    const setCached = async (key, value) => {
        try {
            const db = await openDb();
            await new Promise((resolve, reject) => {
                const tx = db.transaction(STORE, 'readwrite');
                const store = tx.objectStore(STORE);
                const req = store.put(value, key);
                req.onsuccess = () => resolve();
                req.onerror = () => reject(req.error);
            });
        } catch {
            // Best-effort cache only
        }
    };

    const formatNum = (value) => {
        const num = Number(value || 0);
        return Number.isFinite(num)
            ? num.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
            : '0.00';
    };

    window.addEventListener('DOMContentLoaded', () => {
        const form = document.getElementById('upload-form');
        const fileInput = document.getElementById('file-input');
        const btnUpload = document.getElementById('btn-upload');
        const metaInput = document.getElementById('client-meta');

        const panel = document.getElementById('upload-preprocess');
        const status = document.getElementById('preprocess-status');
        const percent = document.getElementById('preprocess-percent');
        const bar = document.getElementById('preprocess-bar');
        const summary = document.getElementById('preprocess-summary');

        if (!form || !fileInput || !btnUpload || !panel || !status || !percent || !bar || !summary || !metaInput) {
            return;
        }

        let worker = null;
        let isProcessing = false;

        const setProgress = (value) => {
            const p = Math.max(0, Math.min(100, Math.round(value || 0)));
            percent.textContent = `${p}%`;
            bar.style.width = `${p}%`;
        };

        const fileCacheKey = (file) => `${file.name}:${file.size}:${file.lastModified}`;

        const renderMetadata = (metadata, source = 'processed') => {
            const sourceText = source === 'cache' ? 'Loaded from browser cache.' : 'Processed on your machine.';
            summary.innerHTML = [
                sourceText,
                `Rows: <strong>${(metadata.rowCount || 0).toLocaleString()}</strong>`,
                `Pricing Total: <strong>${formatNum(metadata.pricingTotal)}</strong>`,
                `Billing Total: <strong>${formatNum(metadata.billingTotal)}</strong>`,
                `Invoices: <strong>${(metadata.invoiceCount || 0).toLocaleString()}</strong>`,
            ].join(' &nbsp;|&nbsp; ');
            metaInput.value = JSON.stringify(metadata);
        };

        const stopWorker = () => {
            if (worker) {
                worker.terminate();
                worker = null;
            }
        };

        fileInput.addEventListener('change', async () => {
            const file = fileInput.files && fileInput.files[0];
            stopWorker();
            metaInput.value = '';

            if (!file) {
                panel.style.display = 'none';
                isProcessing = false;
                btnUpload.disabled = false;
                return;
            }

            panel.style.display = 'block';
            status.textContent = 'Checking cache...';
            setProgress(0);
            summary.textContent = '';
            isProcessing = true;
            btnUpload.disabled = true;

            const key = fileCacheKey(file);
            const cached = await getCached(key);
            if (cached) {
                status.textContent = 'Ready to upload';
                setProgress(100);
                renderMetadata(cached, 'cache');
                isProcessing = false;
                btnUpload.disabled = false;
                return;
            }

            status.textContent = 'Parsing CSV on your machine...';
            worker = new Worker('/static/js/csvPreviewWorker.js');

            worker.onmessage = async (event) => {
                const data = event.data || {};
                if (data.type === 'progress') {
                    setProgress(data.progress || 0);
                    status.textContent = `Parsing CSV on your machine... (${(data.rowCount || 0).toLocaleString()} rows)`;
                    return;
                }

                if (data.type === 'done') {
                    const metadata = data.metadata || {};
                    status.textContent = 'Ready to upload';
                    setProgress(100);
                    renderMetadata(metadata, 'processed');
                    await setCached(key, metadata);
                    isProcessing = false;
                    btnUpload.disabled = false;
                    stopWorker();
                    return;
                }

                if (data.type === 'error') {
                    status.textContent = 'Preprocess failed, you can still upload';
                    setProgress(0);
                    summary.textContent = data.message || 'Could not process locally.';
                    isProcessing = false;
                    btnUpload.disabled = false;
                    stopWorker();
                }
            };

            worker.onerror = () => {
                status.textContent = 'Preprocess failed, you can still upload';
                setProgress(0);
                summary.textContent = 'Worker failed unexpectedly.';
                isProcessing = false;
                btnUpload.disabled = false;
                stopWorker();
            };

            worker.postMessage({ file, chunkSize: 8 * 1024 * 1024 });
        });

        form.addEventListener('submit', (event) => {
            if (isProcessing) {
                event.preventDefault();
                alert('Please wait for local CSV processing to finish before uploading.');
            }
        });
    });
})();
