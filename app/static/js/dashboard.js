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
        vat: parseFloat(container.dataset.defaultVat || '1.12') || 1.12,
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
    const columnControls = document.getElementById('column-controls');
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
    vatInput.value = defaults.vat;

    let totalRecords = 0;
    let activeFilters = {};
    let activeSearch = '';
    const domainMap = new Map();
    let allDomains = new Set();
    let invoiceOptionsLoaded = false;

    const resetFiltersForUploadChange = () => {
        activeFilters = {};
        activeSearch = '';
        if (searchInput) {
            searchInput.value = '';
        }
        domainMap.clear();
        allDomains = new Set();
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

    const syncColumnCheckbox = (column, isVisible) => {
        if (!columnControls || !column) return;
        const field = column.getField ? column.getField() : column.field;
        if (!field) return;
        const checkbox = columnControls.querySelector(`input[data-field="${field}"]`);
        if (checkbox) {
            checkbox.checked = isVisible;
        }
        window.requestAnimationFrame(updateSliderVisibility);
    };

    const renderColumnControls = () => {
        if (!columnControls) return;
        columnControls.querySelectorAll('.column-controls__option, .column-controls__group-label').forEach((node) => node.remove());

        const groups = [
            { key: 'entitlement', label: 'Entitlement Columns' },
            { key: 'meter', label: 'Meter Columns' },
            { key: 'default', label: 'Other Columns' },
        ];

        const columnMeta = getColumnControlMeta();

        const groupedDefs = columnMeta.reduce((acc, def) => {
            const key = def.controlGroup || 'default';
            if (!acc[key]) {
                acc[key] = [];
            }
            acc[key].push(def);
            return acc;
        }, {});

        groups.forEach(({ key, label }) => {
            const defs = groupedDefs[key];
            if (!defs || defs.length === 0) {
                return;
            }

            const heading = document.createElement('span');
            heading.className = 'column-controls__group-label';
            heading.textContent = label;
            columnControls.appendChild(heading);

            defs.forEach((def) => {
                if (!def.field) return;
                const option = document.createElement('label');
                option.className = 'column-controls__option';
                const checkbox = document.createElement('input');
                checkbox.type = 'checkbox';
                checkbox.dataset.field = def.field;
                const column = table ? table.getColumn(def.field) : null;
                const columnVisible = column && typeof column.isVisible === 'function'
                    ? column.isVisible()
                    : true;
                checkbox.checked = columnVisible !== false;
                checkbox.addEventListener('change', () => {
                    if (checkbox.checked) {
                        table.showColumn(def.field);
                    } else {
                        table.hideColumn(def.field);
                    }
                    window.requestAnimationFrame(updateSliderVisibility);
                });
                const text = document.createElement('span');
                text.textContent = def.title;
                option.append(checkbox, text);
                columnControls.appendChild(option);
            });
        });
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
        renderColumnControls();
        window.requestAnimationFrame(updateSliderVisibility);
    });
    bindScrollSlider();
    window.requestAnimationFrame(updateSliderVisibility);

    window.addEventListener('resize', () => window.requestAnimationFrame(updateSliderVisibility));

    table.on('columnHidden', (column) => syncColumnCheckbox(column, false));
    table.on('columnShown', (column) => syncColumnCheckbox(column, true));
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

    const updateDropdownOptions = (records) => {
        const maxOptions = 50;
        const collectors = {};
        dropdowns.forEach((dropdown) => {
            const field = dropdown.dataset.filter;
            if (!field || field === 'CustomerDomainName' || field === 'InvoiceNumber') {
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
            if (domainValue && allDomains.size < maxOptions) {
                allDomains.add(domainValue);
            }
            if (customerName && domainValue) {
                if (!domainMap.has(customerName)) {
                    domainMap.set(customerName, new Set());
                }
                const domainSet = domainMap.get(customerName);
                if (domainSet.size < maxOptions) {
                    domainSet.add(domainValue);
                }
            }
        });

        dropdowns.forEach((dropdown) => {
            const field = dropdown.dataset.filter;
            if (!field || field === 'CustomerDomainName' || field === 'InvoiceNumber') {
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
    };

    const refreshInvoiceDropdown = async (force = false) => {
        if (!invoiceDropdown || !currentUploadId) return;
        if (force) {
            invoiceOptionsLoaded = false;
        }
        if (invoiceOptionsLoaded) return;

        try {
            const response = await fetch(`/api/uploads/${currentUploadId}/invoices?limit=1000`);
            if (!response.ok) {
                throw new Error('Failed to load invoices');
            }
            const payload = await response.json();
            const invoices = payload.invoices || [];
            const currentValue = invoiceDropdown.value;
            const placeholder = invoiceDropdown.dataset.placeholder || 'Invoice';
            invoiceDropdown.innerHTML = `<option value="">${placeholder}</option>`;
            invoices.forEach((invoice) => {
                const option = document.createElement('option');
                option.value = invoice;
                option.textContent = invoice;
                invoiceDropdown.appendChild(option);
            });
            const nextValue = currentValue && invoices.includes(currentValue) ? currentValue : '';
            invoiceDropdown.value = nextValue;
            if (nextValue) {
                activeFilters.invoice = nextValue;
            } else {
                delete activeFilters.invoice;
            }
            invoiceOptionsLoaded = true;
        } catch (error) {
            console.error(error);
        }
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

    const fetchData = async () => {
        if (!ensureUploadSelected()) {
            return;
        }
        if (tableInfo) {
            tableInfo.textContent = 'Loading...';
        }
        const searchTerm = searchInput.value?.trim() || '';
        const { params: filterParams } = readFilters();
        activeFilters = { ...filterParams };
        activeSearch = searchTerm;

        const queryParams = {
            page: 1,
            search: searchTerm,
            forex: forexInput.value,
            margin: marginInput.value,
            vat: vatInput.value,
            all_records: true,
        };
        Object.entries(filterParams).forEach(([key, value]) => {
            queryParams[key] = value;
        });
        const query = buildQuery(queryParams);
        const response = await fetch(`/api/uploads/${currentUploadId}/data?${query}`);
        if (!response.ok) {
            throw new Error('Failed to load data');
        }
        const payload = await response.json();
        totalRecords = payload.total;
        table.setData(payload.records);
        const shouldUpdateOptions = Object.keys(activeFilters).length === 0 && !activeSearch;
        if (shouldUpdateOptions) {
            updateDropdownOptions(payload.records);
        }
        updateTableInfo();
        await refreshInvoiceDropdown(true);
        window.requestAnimationFrame(updateSliderVisibility);
    };

    const updateSummary = async () => {
        if (!currentUploadId) {
            setSummaryPlaceholders();
            return;
        }
        const summaryParams = {
            forex: forexInput.value,
            margin: marginInput.value,
            vat: vatInput.value,
        };
        Object.entries(activeFilters).forEach(([key, value]) => {
            summaryParams[key] = value;
        });
        if (activeSearch) {
            summaryParams.search = activeSearch;
        }
        const params = buildQuery(summaryParams);
        const response = await fetch(`/api/uploads/${currentUploadId}/summary?${params}`);
        if (!response.ok) {
            throw new Error('Failed to load summary');
        }
        const summary = await response.json();
        const scopedView = Boolean(activeFilters.customer && activeFilters.customer_domain);
        setSummaryValues(
            scopedView ? '--' : formatterCurrency(summary.total_pricing),
            scopedView ? '--' : formatterCurrency(summary.total_billing),
            scopedView ? '--' : formatterCurrency(summary.total_vat_ex),
            formatterCurrency(summary.total_vat_inc),
        );
    };

    const updateCharts = async () => {
        if (!currentUploadId) {
            const existingChart = Chart.getChart('chart-customers');
            if (existingChart) existingChart.destroy();
            return;
        }
        const chartParams = { ...activeFilters };
        if (activeSearch) {
            chartParams.search = activeSearch;
        }
        const querySuffix = Object.keys(chartParams).length ? `?${buildQuery(chartParams)}` : '';
        const customersRes = await fetch(`/api/uploads/${currentUploadId}/top-customers${querySuffix}`);
        const customers = customersRes.ok ? await customersRes.json() : [];

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

    const refreshAll = async () => {
        try {
            await fetchData();
            await Promise.all([updateSummary(), updateCharts()]);
        } catch (error) {
            console.error(error);
            if (tableInfo) {
                tableInfo.textContent = 'Failed to load data.';
            }
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

    btnRefresh.addEventListener('click', () => {
        fetchData()
            .then(() => Promise.all([updateSummary(), updateCharts()]))
            .catch(console.error);
    });

    const applyFilters = () => {
        fetchData()
            .then(() => Promise.all([updateSummary(), updateCharts()]))
            .catch(console.error);
    };

    btnApply.addEventListener('click', applyFilters);
    searchInput.addEventListener('change', applyFilters);
    if (customerDropdown) {
        customerDropdown.addEventListener('change', () => {
            renderDomainOptions();
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
        showNoUploadState('No completed uploads yet.');
    }
};

window.addEventListener('DOMContentLoaded', initDashboard);
