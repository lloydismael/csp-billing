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

const initDashboard = () => {
    const container = document.querySelector('.page-grid');
    if (!container) {
        return;
    }

    const uploadSelector = document.getElementById('select-upload');
    let currentUploadId = '';
    if (uploadSelector && uploadSelector.value) {
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

    const columnDefs = [
        { title: 'Customer', field: 'CustomerName', width: 220 },
        { title: 'Domain', field: 'CustomerDomainName', width: 220 },
        { title: 'Entitlement', field: 'EntitlementDescription', width: 220, headerFilter: 'input', headerFilterPlaceholder: 'Filter entitlement', controlGroup: 'entitlement' },
        { title: 'Entitlement ID', field: 'EntitlementId', width: 200, headerFilter: 'input', headerFilterPlaceholder: 'Filter entitlement id', controlGroup: 'entitlement' },
        { title: 'Tags', field: 'Tags', width: 220 },
        { title: 'Invoice', field: 'InvoiceNumber', width: 140 },
        { title: 'Product', field: 'ProductName', width: 220 },
        { title: 'Meter Category', field: 'MeterCategory', width: 160, headerFilter: 'input', headerFilterPlaceholder: 'Filter category', controlGroup: 'meter' },
        { title: 'Meter Subcategory', field: 'MeterSubCategory', width: 180, headerFilter: 'input', headerFilterPlaceholder: 'Filter subcategory', controlGroup: 'meter' },
        { title: 'Meter Name', field: 'MeterName', width: 200, headerFilter: 'input', headerFilterPlaceholder: 'Filter name', controlGroup: 'meter' },
        { title: 'Meter Type', field: 'MeterType', width: 160, headerFilter: 'input', headerFilterPlaceholder: 'Filter type', controlGroup: 'meter' },
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

    const getColumnControlMeta = () => {
        if (table && typeof table.getColumns === 'function') {
            return table.getColumns()
                .map((column) => {
                    const definition = column.getDefinition ? column.getDefinition() || {} : {};
                    const field = definition.field || (column.getField ? column.getField() : undefined);
                    if (!field) {
                        return null;
                    }
                    return {
                        field,
                        title: definition.title || field,
                        controlGroup: definition.controlGroup || 'default',
                    };
                })
                .filter(Boolean);
        }
        return columnDefs
            .filter((def) => Boolean(def.field))
            .map((def) => ({
                field: def.field,
                title: def.title || def.field,
                controlGroup: def.controlGroup || 'default',
            }));
    };

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
        table.clearData();
        if (tableScrollContainer) {
            tableScrollContainer.scrollLeft = 0;
        }
        if (tableInfo) {
            tableInfo.textContent = message || 'Select a billing file to view data.';
        }
        setSummaryPlaceholders();
        const existingChart = Chart.getChart('chart-customers');
        if (existingChart) existingChart.destroy();
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

    const renderTopCustomersFromRecords = (records) => {
        const grouped = new Map();
        (records || []).forEach((row) => {
            const label = row.CustomerName || 'Unknown';
            const current = grouped.get(label) || 0;
            grouped.set(label, current + (Number(row.PricingPreTaxTotal || 0) || 0));
        });

        const customers = Array.from(grouped.entries())
            .map(([label, value]) => ({ label, value }))
            .sort((a, b) => b.value - a.value)
            .slice(0, 10);

        const customerChart = Chart.getChart('chart-customers');
        if (customerChart) customerChart.destroy();
        new Chart(document.getElementById('chart-customers'), {
            type: 'doughnut',
            data: {
                labels: customers.map((c) => c.label),
                datasets: [{
                    data: customers.map((c) => c.value),
                    backgroundColor: ['#0f81c7', '#4ac2ff', '#8bdcf9', '#2762d3', '#1c46a7', '#133577', '#1c9ac7'],
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { position: 'right' } },
            },
        });
    };

    const applyClientView = () => {
        const filtered = filterClientRecords(allRecords);
        const transformed = filtered.map(computeDerivedRecord);

        totalRecords = transformed.length;
        table.setData(transformed);
        updateTableInfo();
        renderSummaryFromRecords(transformed);
        renderTopCustomersFromRecords(transformed);
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

    const refreshAll = async () => {
        const loadingOverlay = document.getElementById('loading-overlay');
        const loadingProgress = document.getElementById('loading-progress');
        
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
                const cachedRecords = getUploadCache(currentUploadId);
                if (cachedRecords && cachedRecords.length > 0) {
                    allRecords = cachedRecords;
                    loadedUploadId = currentUploadId;
                    applyClientView();
                } else if (loadedUploadId === currentUploadId && allRecords.length > 0) {
                    applyClientView();
                } else {
                    await fetchData();
                }
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
            table.clearData();
            setSummaryPlaceholders();
            if (tableScrollContainer) {
                tableScrollContainer.scrollLeft = 0;
            }
            window.requestAnimationFrame(updateSliderVisibility);
            if (tableInfo) {
                tableInfo.textContent = currentUploadId ? 'Loading...' : 'Select a billing file to view data.';
            }
            if (currentUploadId) {
                refreshAll();
            } else {
                showNoUploadState();
            }
        });
    }

    btnRefresh.addEventListener('click', refreshAll);

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
    };

    btnApply.addEventListener('click', applyFilters);
    searchInput.addEventListener('change', applyFilters);
    if (customerDropdown) {
        customerDropdown.addEventListener('change', () => {
            renderDomainOptions();
            renderSubOptions();
        });
    }
    if (domainDropdown) {
        domainDropdown.addEventListener('change', () => {
            renderSubOptions();
        });
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
        refreshAll();
    } else {
        showNoUploadState('Please select a billing file to start.');
    }
};

window.addEventListener('DOMContentLoaded', initDashboard);
