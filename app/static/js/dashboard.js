const formatterCurrency = (value) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
        return '0.00';
    }
    return Number(value).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
};

const buildQuery = (params) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, val]) => {
        if (val !== undefined && val !== null && val !== '') {
            query.append(key, val);
        }
    });
    return query.toString();
};

const DASHBOARD_STATE_KEY = 'csp_dashboard_state_v2';
const DASHBOARD_RECORDS_PREFIX = 'csp_dashboard_records_v3_';
const DASHBOARD_CACHE_DB = 'csp-dashboard-cache';
const DASHBOARD_CACHE_STORE = 'records';
const DASHBOARD_CACHE_VERSION = 1;
const DASHBOARD_CACHE_MAX_ITEMS = 5;

// Prefer localStorage for lightweight UI state only. Large billing records are stored in
// IndexedDB so page navigation does not force a billing-file re-download.
const getStorage = () => {
    try {
        const probeKey = '__csp_probe__';
        window.localStorage.setItem(probeKey, '1');
        window.localStorage.removeItem(probeKey);
        return window.localStorage;
    } catch (err) {
        try {
            return window.sessionStorage;
        } catch (_) {
            return null;
        }
    }
};

const readUploadMeta = () => {
    const metaEl = document.getElementById('dashboard-upload-meta');
    if (!metaEl) return {};
    try {
        const parsed = JSON.parse(metaEl.textContent || '{}');
        return parsed && typeof parsed === 'object' ? parsed : {};
    } catch (_) {
        return {};
    }
};

const getUploadCacheVersion = (uploadId) => {
    const meta = readUploadMeta();
    return meta[String(uploadId || '')]?.cache_version || '';
};

const makeRecordCacheKey = (uploadId, cacheVersion) => {
    if (!uploadId || !cacheVersion) return '';
    return `${DASHBOARD_RECORDS_PREFIX}${uploadId}:${cacheVersion}`;
};

const openDashboardCacheDb = () => new Promise((resolve, reject) => {
    if (!('indexedDB' in window)) {
        reject(new Error('IndexedDB unavailable'));
        return;
    }
    const request = indexedDB.open(DASHBOARD_CACHE_DB, DASHBOARD_CACHE_VERSION);
    request.onupgradeneeded = () => {
        const db = request.result;
        if (!db.objectStoreNames.contains(DASHBOARD_CACHE_STORE)) {
            const store = db.createObjectStore(DASHBOARD_CACHE_STORE, { keyPath: 'key' });
            store.createIndex('savedAt', 'savedAt', { unique: false });
        }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error || new Error('IndexedDB open failed'));
});

const readPersistedState = () => {
    const storage = getStorage();
    if (!storage) return null;
    try {
        const raw = storage.getItem(DASHBOARD_STATE_KEY);
        if (!raw) return null;
        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === 'object' ? parsed : null;
    } catch (err) {
        return null;
    }
};

const writePersistedState = (state) => {
    const storage = getStorage();
    if (!storage) return;
    try {
        storage.setItem(DASHBOARD_STATE_KEY, JSON.stringify(state || {}));
    } catch (err) {
        /* storage may be full or unavailable; ignore */
    }
};

const cleanupPersistedRecords = async () => {
    try {
        const db = await openDashboardCacheDb();
        const items = await new Promise((resolve, reject) => {
            const tx = db.transaction(DASHBOARD_CACHE_STORE, 'readonly');
            const store = tx.objectStore(DASHBOARD_CACHE_STORE);
            const request = store.getAll();
            request.onsuccess = () => resolve(request.result || []);
            request.onerror = () => reject(request.error);
        });
        const sorted = items.sort((a, b) => Number(b.savedAt || 0) - Number(a.savedAt || 0));
        const stale = sorted.slice(DASHBOARD_CACHE_MAX_ITEMS);
        if (!stale.length) return;
        await new Promise((resolve, reject) => {
            const tx = db.transaction(DASHBOARD_CACHE_STORE, 'readwrite');
            const store = tx.objectStore(DASHBOARD_CACHE_STORE);
            stale.forEach((item) => store.delete(item.key));
            tx.oncomplete = () => resolve();
            tx.onerror = () => reject(tx.error);
        });
    } catch (_) {
        // Cache cleanup is best-effort only.
    }
};

const readPersistedRecords = async (uploadId) => {
    const cacheVersion = getUploadCacheVersion(uploadId);
    const key = makeRecordCacheKey(uploadId, cacheVersion);
    if (!key) return null;
    try {
        const db = await openDashboardCacheDb();
        const item = await new Promise((resolve, reject) => {
            const tx = db.transaction(DASHBOARD_CACHE_STORE, 'readonly');
            const store = tx.objectStore(DASHBOARD_CACHE_STORE);
            const request = store.get(key);
            request.onsuccess = () => resolve(request.result || null);
            request.onerror = () => reject(request.error);
        });
        return item && Array.isArray(item.records) ? item.records : null;
    } catch (_) {
        return null;
    }
};

const writePersistedRecords = async (uploadId, records) => {
    const cacheVersion = getUploadCacheVersion(uploadId);
    const key = makeRecordCacheKey(uploadId, cacheVersion);
    if (!key) return;
    try {
        const db = await openDashboardCacheDb();
        await new Promise((resolve, reject) => {
            const tx = db.transaction(DASHBOARD_CACHE_STORE, 'readwrite');
            const store = tx.objectStore(DASHBOARD_CACHE_STORE);
            store.put({
                key,
                uploadId: String(uploadId),
                cacheVersion,
                records: records || [],
                rowCount: Array.isArray(records) ? records.length : 0,
                savedAt: Date.now(),
            });
            tx.oncomplete = () => resolve();
            tx.onerror = () => reject(tx.error);
        });
        cleanupPersistedRecords();
    } catch (_) {
        // Large-record persistence is an optimization. Ignore cache failures.
    }
};

const initDashboard = async () => {
    const container = document.querySelector('.page-grid');
    if (!container) {
        return;
    }

    const uploadSelector = document.getElementById('select-upload');
    const persistedState = readPersistedState() || {};
    let currentUploadId = '';
    const availableUploadIds = uploadSelector
        ? Array.from(uploadSelector.options).map((opt) => opt.value).filter(Boolean)
        : [];
    const persistedUploadId = persistedState.uploadId ? String(persistedState.uploadId) : '';
    if (persistedUploadId && availableUploadIds.includes(persistedUploadId)) {
        currentUploadId = persistedUploadId;
        if (uploadSelector) uploadSelector.value = persistedUploadId;
    } else if (uploadSelector && uploadSelector.value) {
        currentUploadId = uploadSelector.value;
    } else if (container.dataset.latestUpload) {
        currentUploadId = container.dataset.latestUpload;
        if (uploadSelector) {
            uploadSelector.value = currentUploadId;
        }
    }

    const defaults = {
        forex: parseFloat(container.dataset.defaultForex || '1') || 1.0,
        margin: parseFloat(container.dataset.defaultMargin || '1') || 1.0,
        vat: (parseFloat(container.dataset.defaultVat || '1.12') || 1.12) <= 0 ? 0 : 1.12,
    };

    const forexInput = document.getElementById('input-forex');
    const marginInput = document.getElementById('input-margin');
    const vatInput = document.getElementById('input-vat');
    const searchInput = document.getElementById('search-box');
    const btnRefresh = document.getElementById('btn-refresh');
    const btnApply = document.getElementById('btn-apply-filters');
    const btnInvoice = document.getElementById('btn-invoice');
    const btnExport = document.getElementById('btn-export');
    const tableInfo = document.getElementById('table-info');
    const dropdowns = document.querySelectorAll('select[data-filter]');
    const customerDropdown = document.getElementById('filter-customer');
    const domainDropdown = document.getElementById('filter-domain');
    const domainPlaceholder = domainDropdown?.querySelector('option:first-child')?.textContent || 'Domain';
    if (domainDropdown && !domainDropdown.dataset.placeholder) {
        domainDropdown.dataset.placeholder = domainPlaceholder;
    }
    const invoiceDropdown = document.getElementById('filter-invoice');
    const invoicePlaceholder = invoiceDropdown?.querySelector('option:first-child')?.textContent || 'Invoice';
    if (invoiceDropdown) {
        invoiceDropdown.dataset.placeholder = invoicePlaceholder;
    }
    const tableScrollContainer = document.querySelector('.table-scroll');
    const scrollSlider = document.getElementById('table-scroll-slider');
    const scrollSliderWrapper = document.getElementById('table-scroll-slider-wrapper');
    const summaryNodes = {
        pricing: document.querySelector('#card-pricing .value'),
        billing: document.querySelector('#card-billing .value'),
        vatEx: document.querySelector('#card-vatex .value'),
        vatInc: document.querySelector('#card-vatinc .value'),
    };

    dropdowns.forEach((dropdown) => {
        if (!dropdown) return;
        const placeholderText = dropdown.querySelector('option:first-child')?.textContent || '';
        if (!dropdown.dataset.placeholder) {
            dropdown.dataset.placeholder = placeholderText;
        }
    });

    const setSummaryValues = (pricing, billing, vatEx, vatInc) => {
        if (summaryNodes.pricing) summaryNodes.pricing.textContent = pricing;
        if (summaryNodes.billing) summaryNodes.billing.textContent = billing;
        if (summaryNodes.vatEx) summaryNodes.vatEx.textContent = vatEx;
        if (summaryNodes.vatInc) summaryNodes.vatInc.textContent = vatInc;
    };

    const setSummaryPlaceholders = () => {
        setSummaryValues('--', '--', '--', '--');
    };

    forexInput.value = defaults.forex;
    marginInput.value = defaults.margin;
    if (vatInput) {
        vatInput.value = String(defaults.vat);
    }

    // Apply persisted input values (if any) on top of defaults so the user's last
    // forex / margin / VAT choices survive navigation to other pages.
    if (persistedState.forex !== undefined && persistedState.forex !== null && persistedState.forex !== '') {
        forexInput.value = persistedState.forex;
    }
    if (persistedState.margin !== undefined && persistedState.margin !== null && persistedState.margin !== '') {
        marginInput.value = persistedState.margin;
    }
    if (vatInput && persistedState.vat !== undefined && persistedState.vat !== null && persistedState.vat !== '') {
        vatInput.value = String(persistedState.vat);
    }
    if (searchInput && persistedState.search) {
        searchInput.value = persistedState.search;
    }

    let totalRecords = 0;
    let activeFilters = {};
    let activeSearch = '';
    let allRecords = [];
    let loadedUploadId = '';
    const uploadDataCache = new Map();
    const MAX_UPLOAD_CACHE_ITEMS = 5;
    const domainMap = new Map();
    let allDomains = new Set();
    const customerSubMap = new Map();
    const domainSubMap = new Map();
    let allSubs = new Set();
    let invoiceOptionsLoaded = false;
    
    // new select node
    const subDropdown = document.getElementById('filter-subscription');
    const subPlaceholder = subDropdown?.querySelector('option:first-child')?.textContent || 'Subscription';
    if (subDropdown && !subDropdown.dataset.placeholder) {
        subDropdown.dataset.placeholder = subPlaceholder;
    }

    const resetFiltersForUploadChange = () => {
        activeFilters = {};
        activeSearch = '';
        if (searchInput) searchInput.value = '';
        domainMap.clear();
        allDomains = new Set();
        customerSubMap.clear();
        domainSubMap.clear();
        allSubs = new Set();
        dropdowns.forEach((dropdown) => {
            if (!dropdown) return;
            const placeholderText = dropdown.dataset.placeholder || '';
            dropdown.innerHTML = `<option value="">${placeholderText}</option>`;
        });
        if (invoiceDropdown) {
            const placeholderText = invoiceDropdown.dataset.placeholder || 'Invoice';
            invoiceDropdown.innerHTML = `<option value="">${placeholderText}</option>`;
        }
        invoiceOptionsLoaded = false;
    };

    const columnControlGroups = {
        EntitlementDescription: 'entitlement',
        EntitlementId: 'entitlement',
        MeterCategory: 'meter',
        MeterSubCategory: 'meter',
        MeterName: 'meter',
        MeterType: 'meter',
    };

    const columnDefs = [
        { title: 'Customer', field: 'CustomerName', width: 220 },
        { title: 'Domain', field: 'CustomerDomainName', width: 220 },
        { title: 'Entitlement', field: 'EntitlementDescription', width: 220, headerFilter: 'input', headerFilterPlaceholder: 'Filter entitlement' },
        { title: 'Entitlement ID', field: 'EntitlementId', width: 200, headerFilter: 'input', headerFilterPlaceholder: 'Filter entitlement id' },
        { title: 'Tags', field: 'Tags', width: 220 },
        { title: 'Invoice', field: 'InvoiceNumber', width: 140 },
        { title: 'Product', field: 'ProductName', width: 220 },
        { title: 'Meter Category', field: 'MeterCategory', width: 160, headerFilter: 'input', headerFilterPlaceholder: 'Filter category' },
        { title: 'Meter Subcategory', field: 'MeterSubCategory', width: 180, headerFilter: 'input', headerFilterPlaceholder: 'Filter subcategory' },
        { title: 'Meter Name', field: 'MeterName', width: 200, headerFilter: 'input', headerFilterPlaceholder: 'Filter name' },
        { title: 'Meter Type', field: 'MeterType', width: 160, headerFilter: 'input', headerFilterPlaceholder: 'Filter type' },
        { title: 'Usage Date', field: 'UsageDate', width: 140 },
        { title: 'Quantity', field: 'Quantity', width: 120, hozAlign: 'right' },
        { title: 'Unit Price', field: 'UnitPrice', width: 120, formatter: (cell) => formatterCurrency(cell.getValue()) },
        { title: 'Pricing PreTax', field: 'PricingPreTaxTotal', width: 140, formatter: (cell) => formatterCurrency(cell.getValue()) },
        { title: 'Billing PreTax', field: 'BillingPreTaxTotal', width: 140, formatter: (cell) => formatterCurrency(cell.getValue()) },
        { title: 'Forex', field: 'Forex', width: 90 },
        { title: 'PreTax w Forex', field: 'PreTaxWithForex', width: 160, formatter: (cell) => formatterCurrency(cell.getValue()) },
        { title: 'Margin', field: 'Margin', width: 100 },
        { title: 'Total VAT EX', field: 'TotalVATEx', width: 150, formatter: (cell) => formatterCurrency(cell.getValue()) },
        { title: 'VAT', field: 'VAT', width: 90 },
        { title: 'Total VAT Inc', field: 'TotalVATInc', width: 160, formatter: (cell) => formatterCurrency(cell.getValue()) },
    ];

    let table;
    let tableReady = false;

    const safeClearTable = () => {
        if (!table || !tableReady || typeof table.clearData !== 'function') {
            return;
        }
        table.clearData();
    };

    const getColumnControlMeta = () => columnDefs
        .filter((def) => Boolean(def.field))
        .map((def) => ({
            field: def.field,
            title: def.title || def.field,
            controlGroup: columnControlGroups[def.field] || 'default',
        }));

    const columnHeaderMenu = () => getColumnControlMeta().map((colDef) => ({
        label: `<span>${colDef.title}</span>`,
        action: () => {
            if (!table) return;
            const targetColumn = table.getColumn(colDef.field);
            if (!targetColumn) return;
            if (typeof targetColumn.isVisible === 'function' && targetColumn.isVisible()) {
                targetColumn.hide();
            } else {
                targetColumn.show();
            }
        }
    }));

    table = new Tabulator('#grid-table', {
        height: '480px',
        layout: 'fitDataStretch',
        columnDefaults: {
            headerMenu: columnHeaderMenu,
        },
        columns: columnDefs,
        placeholder: 'Loading records...'
    });
    table.on('tableBuilt', () => {
        tableReady = true;
    });

    const updateSliderVisibility = () => {
        if (!tableScrollContainer || !scrollSlider || !scrollSliderWrapper) return;
        const maxScroll = Math.max(tableScrollContainer.scrollWidth - tableScrollContainer.clientWidth, 0);
        if (maxScroll > 4) {
            scrollSliderWrapper.style.display = 'flex';
            const ratio = maxScroll ? tableScrollContainer.scrollLeft / maxScroll : 0;
            scrollSlider.value = Math.round(ratio * 100);
        } else {
            scrollSliderWrapper.style.display = 'none';
            scrollSlider.value = 0;
        }
    };

    const showNoUploadState = (message) => {
        resetFiltersForUploadChange();
        totalRecords = 0;
        safeClearTable();
        if (tableScrollContainer) {
            tableScrollContainer.scrollLeft = 0;
        }
        if (tableInfo) {
            tableInfo.textContent = message || 'Select a billing file to view data.';
        }
        setSummaryPlaceholders();
        ['chart-customers', 'chart-domains', 'chart-entitlements', 'chart-meters'].forEach(id => {
            const existingChart = Chart.getChart(id);
            if (existingChart) existingChart.destroy();
        });
        window.requestAnimationFrame(updateSliderVisibility);
    };

    const ensureUploadSelected = () => {
        if (!currentUploadId) {
            showNoUploadState();
            return false;
        }
        return true;
    };



    const bindScrollSlider = () => {
        if (!tableScrollContainer || !scrollSlider) return;
        scrollSlider.addEventListener('input', () => {
            const maxScroll = Math.max(tableScrollContainer.scrollWidth - tableScrollContainer.clientWidth, 0);
            tableScrollContainer.scrollLeft = (scrollSlider.value / 100) * maxScroll;
        });
        tableScrollContainer.addEventListener('scroll', () => {
            window.requestAnimationFrame(updateSliderVisibility);
        });
    };

    table.on('tableBuilt', () => {
        window.requestAnimationFrame(updateSliderVisibility);
    });
    bindScrollSlider();
    window.requestAnimationFrame(updateSliderVisibility);

    window.addEventListener('resize', () => window.requestAnimationFrame(updateSliderVisibility));

    table.on('renderComplete', () => window.requestAnimationFrame(updateSliderVisibility));
    table.on('dataLoaded', () => window.requestAnimationFrame(updateSliderVisibility));

    const renderDomainOptions = () => {
        if (!domainDropdown) return;
        const selectedCustomer = customerDropdown?.value || '';
        const currentDomain = domainDropdown.value;
        const values = selectedCustomer
            ? Array.from(domainMap.get(selectedCustomer) || [])
            : Array.from(allDomains);
        const sortedValues = values.sort((a, b) => a.localeCompare(b));
        domainDropdown.innerHTML = `<option value="">${domainPlaceholder}</option>`;
        sortedValues.forEach((value) => {
            const option = document.createElement('option');
            option.value = value;
            option.textContent = value;
            domainDropdown.appendChild(option);
        });
        if (sortedValues.includes(currentDomain)) {
            domainDropdown.value = currentDomain;
        } else {
            domainDropdown.value = '';
        }
    };

    const renderSubOptions = () => {
        if (!subDropdown) return;
        const selectedCustomer = customerDropdown?.value || '';
        const selectedDomain = domainDropdown?.value || '';
        const currentSub = subDropdown.value;
        
        let values;
        if (selectedDomain) {
            values = Array.from(domainSubMap.get(selectedDomain) || []);
        } else if (selectedCustomer) {
            values = Array.from(customerSubMap.get(selectedCustomer) || []);
        } else {
            values = Array.from(allSubs);
        }
        const sortedValues = values.sort((a, b) => a.localeCompare(b));
        subDropdown.innerHTML = `<option value="">${subPlaceholder}</option>`;
        sortedValues.forEach((value) => {
            const option = document.createElement('option');
            option.value = value;
            option.textContent = value;
            subDropdown.appendChild(option);
        });
        if (sortedValues.includes(currentSub)) {
            subDropdown.value = currentSub;
        } else {
            subDropdown.value = '';
        }
    };

    const updateDropdownOptions = (records) => {
        const maxOptions = 50;
        const collectors = {};
        dropdowns.forEach((dropdown) => {
            const field = dropdown.dataset.filter;
            if (!field || field === 'CustomerDomainName' || field === 'EntitlementDescription' || field === 'InvoiceNumber') {
                return;
            }
            if (!collectors[field]) {
                collectors[field] = {
                    values: new Map(),
                    displayField: dropdown.dataset.displayField || '',
                };
            }
        });

        domainMap.clear();
        allDomains = new Set();
        customerSubMap.clear();
        domainSubMap.clear();
        allSubs = new Set();

        records.forEach((record) => {
            Object.entries(collectors).forEach(([field, meta]) => {
                const rawValue = record[field];
                if (rawValue === null || rawValue === undefined) {
                    return;
                }
                const filterValue = String(rawValue);
                if (!filterValue.trim()) {
                    return;
                }
                const alreadyPresent = meta.values.has(filterValue);
                if (!alreadyPresent && meta.values.size >= maxOptions) {
                    return;
                }
                let labelText = meta.values.get(filterValue);
                if (!alreadyPresent) {
                    const displayFieldName = meta.displayField;
                    const displayRaw = displayFieldName ? record[displayFieldName] : undefined;
                    labelText = filterValue;
                    if (displayFieldName) {
                        const displayText = displayRaw ? String(displayRaw).trim() : '';
                        if (displayText) {
                            const normalizedValue = filterValue.trim();
                            labelText = displayText.toLowerCase() === normalizedValue.toLowerCase()
                                ? displayText
                                : `${displayText} (${filterValue})`;
                        }
                    }
                    meta.values.set(filterValue, labelText);
                }

            });

            const customerName = record.CustomerName;
            const domainValue = record.CustomerDomainName;
            const subValue = record.EntitlementDescription;
            if (domainValue && allDomains.size < maxOptions) {
                allDomains.add(domainValue);
            }
            if (subValue && allSubs.size < maxOptions) {
                allSubs.add(subValue);
            }
            if (customerName) {
                if (domainValue) {
                    if (!domainMap.has(customerName)) domainMap.set(customerName, new Set());
                    const domainSet = domainMap.get(customerName);
                    if (domainSet.size < maxOptions) domainSet.add(domainValue);
                }
                if (subValue) {
                    if (!customerSubMap.has(customerName)) customerSubMap.set(customerName, new Set());
                    const cs = customerSubMap.get(customerName);
                    if (cs.size < maxOptions) cs.add(subValue);
                }
            }
            if (domainValue && subValue) {
                if (!domainSubMap.has(domainValue)) domainSubMap.set(domainValue, new Set());
                const ds = domainSubMap.get(domainValue);
                if (ds.size < maxOptions) ds.add(subValue);
            }
        });

        dropdowns.forEach((dropdown) => {
            const field = dropdown.dataset.filter;
            if (!field || field === 'CustomerDomainName' || field === 'EntitlementDescription' || field === 'InvoiceNumber') {
                return;
            }
            const meta = collectors[field];
            if (!meta) {
                return;
            }
            const current = dropdown.value;
            const placeholder = dropdown.querySelector('option:first-child')?.textContent || '';
            dropdown.innerHTML = `<option value="">${placeholder}</option>`;
            const entries = Array.from(meta.values.entries()).sort((a, b) => a[1].localeCompare(b[1]));
            entries.forEach(([value, label]) => {
                const option = document.createElement('option');
                option.value = value;
                option.textContent = label;
                dropdown.appendChild(option);
            });
            dropdown.value = current;
        });

        renderDomainOptions();
        renderSubOptions();
    };

    const updateInvoiceDropdownFromRecords = (records) => {
        if (!invoiceDropdown) return;
        const placeholder = invoiceDropdown.dataset.placeholder || 'Invoice';
        const currentValue = invoiceDropdown.value;
        const invoices = Array.from(
            new Set(
                (records || [])
                    .map((row) => (row && row.InvoiceNumber ? String(row.InvoiceNumber).trim() : ''))
                    .filter((value) => value)
            )
        ).sort((a, b) => a.localeCompare(b));

        invoiceDropdown.innerHTML = `<option value="">${placeholder}</option>`;
        invoices.forEach((invoice) => {
            const option = document.createElement('option');
            option.value = invoice;
            option.textContent = invoice;
            invoiceDropdown.appendChild(option);
        });

        if (currentValue && invoices.includes(currentValue)) {
            invoiceDropdown.value = currentValue;
        } else {
            invoiceDropdown.value = '';
        }

        invoiceOptionsLoaded = true;
    };

    const refreshInvoiceDropdown = async () => {
        if (!invoiceDropdown) return;
        updateInvoiceDropdownFromRecords(allRecords);
    };

    const setUploadCache = (uploadId, records) => {
        const key = String(uploadId || '');
        if (!key) return;

        if (uploadDataCache.has(key)) {
            uploadDataCache.delete(key);
        }
        uploadDataCache.set(key, records || []);

        while (uploadDataCache.size > MAX_UPLOAD_CACHE_ITEMS) {
            const oldestKey = uploadDataCache.keys().next().value;
            uploadDataCache.delete(oldestKey);
        }

        writePersistedRecords(key, records || []);
    };

    const getUploadCache = (uploadId) => {
        const key = String(uploadId || '');
        if (!key || !uploadDataCache.has(key)) {
            return null;
        }

        const value = uploadDataCache.get(key);
        uploadDataCache.delete(key);
        uploadDataCache.set(key, value);
        return value;
    };

    const readFilters = () => {
        const filters = {};
        const params = {};
        dropdowns.forEach((dropdown) => {
            if (dropdown.value) {
                filters[dropdown.dataset.filter] = dropdown.value;
                const paramKey = dropdown.dataset.param || dropdown.dataset.filter.toLowerCase();
                params[paramKey] = dropdown.value;
            }
        });
        return { filters, params };
    };

    const persistState = () => {
        const { params } = readFilters();
        writePersistedState({
            uploadId: currentUploadId || '',
            forex: forexInput ? forexInput.value : '',
            margin: marginInput ? marginInput.value : '',
            vat: vatInput ? vatInput.value : '',
            search: searchInput ? searchInput.value : '',
            customer: params.customer || '',
            customer_domain: params.customer_domain || '',
            invoice: params.invoice || '',
        });
    };

    const restoreFilterSelections = () => {
        const saved = {
            customer: persistedState.customer || '',
            customer_domain: persistedState.customer_domain || '',
            invoice: persistedState.invoice || '',
        };
        dropdowns.forEach((dropdown) => {
            const paramKey = dropdown.dataset.param || '';
            if (!paramKey || !saved[paramKey]) return;
            const value = saved[paramKey];
            const hasOption = Array.from(dropdown.options).some((opt) => opt.value === value);
            if (!hasOption) {
                const opt = document.createElement('option');
                opt.value = value;
                opt.textContent = value;
                dropdown.appendChild(opt);
            }
            dropdown.value = value;
        });
        if (invoiceDropdown && saved.invoice) {
            const hasOption = Array.from(invoiceDropdown.options).some((opt) => opt.value === saved.invoice);
            if (!hasOption) {
                const opt = document.createElement('option');
                opt.value = saved.invoice;
                opt.textContent = saved.invoice;
                invoiceDropdown.appendChild(opt);
            }
            invoiceDropdown.value = saved.invoice;
        }
        if (customerDropdown) {
            renderDomainOptions();
            if (domainDropdown && saved.customer_domain) {
                domainDropdown.value = saved.customer_domain;
            }
        }
    };

    const updateTableInfo = () => {
        if (!currentUploadId) {
            tableInfo.textContent = 'Select a billing file to view data.';
            return;
        }
        tableInfo.textContent = `${totalRecords.toLocaleString()} records`;
    };

    const computeDerivedRecord = (record) => {
        const forex = Number(forexInput.value || defaults.forex || 1) || 1;
        const margin = Number(marginInput.value || defaults.margin || 1) || 1;
        const vat = Number(vatInput.value || defaults.vat || 0) || 0;
        const marginSafe = margin > 0 ? margin : 1;

        const pricingPreTax = Number(record.PricingPreTaxTotal || 0) || 0;
        const preTaxWithForex = pricingPreTax * forex;
        const totalVatEx = preTaxWithForex / marginSafe;
        const vatMultiplier = vat === 0 ? 1 : vat;
        const totalVatInc = totalVatEx * vatMultiplier;

        return {
            ...record,
            Forex: forex,
            Margin: marginSafe,
            VAT: vat,
            PreTaxWithForex: preTaxWithForex,
            TotalVATEx: totalVatEx,
            TotalVATInc: totalVatInc,
        };
    };

    const filterClientRecords = (records) => {
        const searchTerm = (searchInput.value || '').trim().toLowerCase();
        const { params: filterParams } = readFilters();
        activeFilters = { ...filterParams };
        activeSearch = searchTerm;

        return (records || []).filter((row) => {
            if (filterParams.customer && row.CustomerName !== filterParams.customer) return false;
            if (filterParams.customer_domain && row.CustomerDomainName !== filterParams.customer_domain) return false;
            if (filterParams.subscription && row.EntitlementDescription !== filterParams.subscription) return false;
            if (filterParams.invoice && row.InvoiceNumber !== filterParams.invoice) return false;

            if (searchTerm) {
                const haystack = `${row.CustomerName || ''} ${row.CustomerDomainName || ''} ${row.EntitlementDescription || ''} ${row.ProductName || ''} ${row.MeterName || ''}`.toLowerCase();
                if (!haystack.includes(searchTerm)) return false;
            }
            return true;
        });
    };

    const renderSummaryFromRecords = (records) => {
        if (!currentUploadId) {
            setSummaryPlaceholders();
            return;
        }

        const scopedView = Boolean(activeFilters.customer && activeFilters.customer_domain);
        const totals = (records || []).reduce((acc, row) => {
            acc.totalPricing += Number(row.PreTaxWithForex || 0) || 0;
            acc.totalBilling += Number(row.BillingPreTaxTotal || 0) || 0;
            acc.totalVatEx += Number(row.TotalVATEx || 0) || 0;
            acc.totalVatInc += Number(row.TotalVATInc || 0) || 0;
            return acc;
        }, { totalPricing: 0, totalBilling: 0, totalVatEx: 0, totalVatInc: 0 });

        setSummaryValues(
            scopedView ? '--' : formatterCurrency(totals.totalPricing),
            scopedView ? '--' : formatterCurrency(totals.totalBilling),
            scopedView ? '--' : formatterCurrency(totals.totalVatEx),
            formatterCurrency(totals.totalVatInc),
        );
    };

    const renderGenericChart = (records, chartId, extractLabel, chartType = 'doughnut') => {
        try {
            const grouped = new Map();
            (records || []).forEach((row) => {
                const label = extractLabel(row) || 'Unknown';
                const current = grouped.get(label) || 0;
                // Use PreTaxWithForex since that's the base value after exchange rate
                grouped.set(label, current + (Number(row.PreTaxWithForex || row.PricingPreTaxTotal || 0) || 0));
            });

            const items = Array.from(grouped.entries())
                .map(([label, value]) => ({ label, value }))
                .sort((a, b) => b.value - a.value)
                .slice(0, 10);

            const canvas = document.getElementById(chartId);
            if (!canvas) return;

            const existingChart = Chart.getChart(chartId);
            if (existingChart) existingChart.destroy();
            
            const bgColors = ['#0f81c7', '#4ac2ff', '#8bdcf9', '#2762d3', '#1c46a7', '#133577', '#1c9ac7', '#082567', '#1e40af', '#60a5fa'];

            new Chart(canvas, {
                type: chartType,
                data: {
                    labels: items.map((c) => {
                        // Truncate long labels for better display
                        const text = String(c.label);
                        return text.length > 30 ? text.substring(0, 27) + '...' : text;
                    }),
                    datasets: [{
                        data: items.map((c) => Math.max(0, c.value)), // Force non-negative for Chart.js
                        backgroundColor: bgColors,
                    }],
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: chartType === 'doughnut' ? 'right' : 'bottom',
                            display: chartType === 'doughnut'
                        },
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    let label = context.label || '';
                                    if (label) {
                                        label += ': ';
                                    }
                                    if (context.parsed !== null && context.parsed !== undefined) {
                                        const val = typeof context.parsed === 'object' ? context.parsed.y : context.parsed;
                                        label += formatterCurrency(val);
                                    }
                                    return label;
                                }
                            }
                        }
                    },
                    ...(chartType === 'bar' ? {
                        scales: {
                            y: { beginAtZero: true },
                            x: { display: false } // Hide x-axis labels on bar chart if it gets too cluttered, but tooltip works
                        }
                    } : {})
                },
            });
        } catch (err) {
            console.error(`Error rendering chart ${chartId}:`, err);
        }
    };

    const renderChartsFromRecords = (records) => {
        renderGenericChart(records, 'chart-customers', (r) => r.CustomerName, 'doughnut');
        renderGenericChart(records, 'chart-domains', (r) => r.CustomerDomainName, 'doughnut');
        renderGenericChart(records, 'chart-entitlements', (r) => r.EntitlementDescription || r.EntitlementId, 'bar');
        renderGenericChart(records, 'chart-meters', (r) => r.MeterName, 'bar');
    };

    const applyClientView = () => {
        const filtered = filterClientRecords(allRecords);
        const transformed = filtered.map(computeDerivedRecord);

        totalRecords = transformed.length;
        table.setData(transformed);
        updateTableInfo();
        renderSummaryFromRecords(transformed);
        renderChartsFromRecords(transformed);
        window.requestAnimationFrame(updateSliderVisibility);
    };

    const fetchData = async () => {
        if (!ensureUploadSelected()) {
            return;
        }
        if (tableInfo) {
            tableInfo.textContent = 'Loading...';
        }

        const queryParams = {
            page: 1,
            all_records: true,
            include_computed: false,
        };

        const query = buildQuery(queryParams);
        const response = await fetch(`/api/uploads/${currentUploadId}/data?${query}`);
        if (!response.ok) {
            throw new Error('Failed to load data');
        }
        const payload = await response.json();
        allRecords = payload.records || [];
        totalRecords = allRecords.length;
        loadedUploadId = currentUploadId;
        setUploadCache(currentUploadId, allRecords);

        updateDropdownOptions(allRecords);
        updateInvoiceDropdownFromRecords(allRecords);
        applyClientView();
    };

    const refreshAll = async (options = {}) => {
        const loadingOverlay = document.getElementById('loading-overlay');
        const loadingProgress = document.getElementById('loading-progress');
        let cachedRecords = getUploadCache(currentUploadId);
        if (!cachedRecords && !options.force) {
            cachedRecords = await readPersistedRecords(currentUploadId);
            if (cachedRecords && cachedRecords.length > 0) {
                setUploadCache(currentUploadId, cachedRecords);
            }
        }
        const hasCache = Boolean(cachedRecords && cachedRecords.length > 0)
            || (loadedUploadId === currentUploadId && allRecords.length > 0);

        // Fast path: when records are already cached in memory or IndexedDB,
        // render instantly without the loading overlay or a billing-file re-download.
        if (hasCache && !options.force) {
            try {
                if (cachedRecords && cachedRecords.length > 0) {
                    allRecords = cachedRecords;
                    loadedUploadId = currentUploadId;
                }
                updateDropdownOptions(allRecords);
                updateInvoiceDropdownFromRecords(allRecords);
                applyClientView();
                if (tableInfo) {
                    tableInfo.textContent = `${totalRecords.toLocaleString()} records · loaded from cache`;
                }
            } catch (error) {
                console.error(error);
                if (tableInfo) {
                    tableInfo.textContent = 'Failed to load data.';
                }
            }
            return;
        }

        // Enforce visibility
        if (loadingOverlay) {
            loadingOverlay.style.setProperty('display', 'flex', 'important');
            if (loadingProgress) {
                loadingProgress.style.transition = 'none';
                loadingProgress.style.width = '0%';
                void loadingProgress.offsetWidth; // Force paint
                loadingProgress.style.transition = 'width 10s cubic-bezier(0.1, 0.7, 0.1, 1)';
                loadingProgress.style.width = '75%';
            }
        }
        
        // Disable button and update state with high priority styles
        btnRefresh.disabled = true;
        btnRefresh.classList.add('button-disabled');
        btnRefresh.style.setProperty('opacity', '0.5', 'important');
        btnRefresh.style.setProperty('cursor', 'not-allowed', 'important');
        const prevTitle = btnRefresh.title;
        btnRefresh.title = "Currently loading billing file...";
        
        try {
            // Ensure at least 500ms loading time to avoid flickering
            const minLoadTime = new Promise(resolve => setTimeout(resolve, 800));
            const dataLoad = (async () => {
                await fetchData();
            })();
            
            await Promise.all([dataLoad, minLoadTime]);
            
            if (loadingProgress) {
                loadingProgress.style.transition = 'width 0.3s ease';
                loadingProgress.style.width = '100%';
                await new Promise(resolve => setTimeout(resolve, 350));
            }
        } catch (error) {
            console.error(error);
            if (tableInfo) {
                tableInfo.textContent = 'Failed to load data.';
            }
        } finally {
            if (loadingOverlay) {
                loadingOverlay.style.display = 'none';
            }
            if (loadingProgress) {
                loadingProgress.style.width = '0%';
                loadingProgress.style.transition = 'none';
            }
            
            // Re-enable button
            btnRefresh.disabled = false;
            btnRefresh.classList.remove('button-disabled');
            btnRefresh.style.opacity = '';
            btnRefresh.style.cursor = '';
            btnRefresh.title = "Load billing data";
        }
    };

    if (uploadSelector) {
        uploadSelector.addEventListener('change', () => {
            currentUploadId = uploadSelector.value;
            resetFiltersForUploadChange();
            totalRecords = 0;
            safeClearTable();
            setSummaryPlaceholders();
            if (tableScrollContainer) {
                tableScrollContainer.scrollLeft = 0;
            }
            window.requestAnimationFrame(updateSliderVisibility);
            if (tableInfo) {
                tableInfo.textContent = currentUploadId ? 'Loading...' : 'Select a billing file to view data.';
            }
            persistState();
            if (currentUploadId) {
                refreshAll();
            } else {
                showNoUploadState();
            }
        });
    }

    btnRefresh.addEventListener('click', () => {
        persistState();
        refreshAll({ force: true });
    });

    const applyFilters = () => {
        if (!currentUploadId) {
            showNoUploadState('Select a billing file to view data.');
            return;
        }

        if (loadedUploadId !== currentUploadId || allRecords.length === 0) {
            refreshAll();
            return;
        }

        applyClientView();
        persistState();
    };

    btnApply.addEventListener('click', applyFilters);
    searchInput.addEventListener('change', applyFilters);
    if (customerDropdown) {
        customerDropdown.addEventListener('change', () => {
            renderDomainOptions();
            renderSubOptions();
            persistState();
        });
    }
    if (domainDropdown) {
        domainDropdown.addEventListener('change', () => {
            renderSubOptions();
            persistState();
        });
    }
    [forexInput, marginInput, vatInput].forEach((input) => {
        if (!input) return;
        input.addEventListener('change', persistState);
    });
    dropdowns.forEach((dropdown) => {
        if (!dropdown) return;
        dropdown.addEventListener('change', persistState);
    });
    if (invoiceDropdown) {
        invoiceDropdown.addEventListener('change', persistState);
    }
    if (btnInvoice) {
        btnInvoice.addEventListener('click', () => {
            if (!currentUploadId) {
                alert('Select a billing file before generating an invoice.');
                return;
            }
            const { params } = readFilters();
            if (!params.customer || !params.customer_domain) {
                alert('Select a customer and domain before generating the invoice.');
                return;
            }
            const invoiceParams = {
                upload_id: currentUploadId,
                customer: params.customer,
                customer_domain: params.customer_domain,
            };
            invoiceParams.forex = forexInput.value || defaults.forex;
            invoiceParams.margin = marginInput.value || defaults.margin;
            invoiceParams.vat = vatInput.value || defaults.vat;
            if (params.invoice) {
                invoiceParams.invoice = params.invoice;
            }
            const searchTerm = searchInput.value?.trim();
            if (searchTerm) {
                invoiceParams.search = searchTerm;
            }
            const invoiceUrl = `/invoice?${buildQuery(invoiceParams)}`;
            window.open(invoiceUrl, '_blank');
        });
    }
    btnExport.addEventListener('click', () => {
        if (!currentUploadId) {
            alert('Select a billing file before exporting.');
            return;
        }
        table.download('csv', `upload-${currentUploadId}.csv`);
    });

    if (currentUploadId) {
        const hydrated = await readPersistedRecords(currentUploadId);
        if (hydrated && hydrated.length > 0) {
            setUploadCache(currentUploadId, hydrated);
        }
        refreshAll().then(() => {
            restoreFilterSelections();
            applyClientView();
            persistState();
        }).catch(() => { /* handled inside refreshAll */ });
    } else {
        showNoUploadState('Please select a billing file to start.');
    }
};

window.addEventListener('DOMContentLoaded', () => {
    initDashboard().catch((error) => console.error(error));
});
