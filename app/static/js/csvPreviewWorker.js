self.onmessage = async (event) => {
    const { file, chunkSize = 8 * 1024 * 1024 } = event.data || {};

    if (!file) {
        self.postMessage({ type: 'error', message: 'No file provided.' });
        return;
    }

    try {
        const decoder = new TextDecoder('utf-8');
        let offset = 0;
        let leftover = '';
        let headers = null;
        let index = {};

        let rowCount = 0;
        let pricingTotal = 0;
        let billingTotal = 0;
        const invoiceSet = new Set();
        const topCustomers = new Map();

        let lastProgressTs = 0;

        const parseCsvLine = (line) => {
            const fields = [];
            let current = '';
            let inQuotes = false;

            for (let i = 0; i < line.length; i++) {
                const char = line[i];
                const next = line[i + 1];

                if (char === '"') {
                    if (inQuotes && next === '"') {
                        current += '"';
                        i++;
                    } else {
                        inQuotes = !inQuotes;
                    }
                    continue;
                }

                if (char === ',' && !inQuotes) {
                    fields.push(current);
                    current = '';
                    continue;
                }

                current += char;
            }

            fields.push(current);
            return fields;
        };

        const toNumber = (value) => {
            if (value === null || value === undefined) return 0;
            const normalized = String(value).replace(/,/g, '').trim();
            if (!normalized) return 0;
            const num = Number(normalized);
            return Number.isFinite(num) ? num : 0;
        };

        const processLine = (rawLine) => {
            const line = rawLine.replace(/\r$/, '');
            if (!line.trim()) return;

            if (!headers) {
                headers = parseCsvLine(line);
                index = {
                    customer: headers.indexOf('CustomerName'),
                    invoice: headers.indexOf('InvoiceNumber'),
                    pricing: headers.indexOf('PricingPreTaxTotal'),
                    billing: headers.indexOf('BillingPreTaxTotal'),
                };
                return;
            }

            const cells = parseCsvLine(line);
            rowCount += 1;

            const pricing = index.pricing >= 0 ? toNumber(cells[index.pricing]) : 0;
            const billing = index.billing >= 0 ? toNumber(cells[index.billing]) : 0;
            pricingTotal += pricing;
            billingTotal += billing;

            if (index.invoice >= 0) {
                const invoice = String(cells[index.invoice] || '').trim();
                if (invoice) invoiceSet.add(invoice);
            }

            if (index.customer >= 0) {
                const customer = String(cells[index.customer] || '').trim() || 'Unknown';
                topCustomers.set(customer, (topCustomers.get(customer) || 0) + pricing);
            }
        };

        while (offset < file.size) {
            const end = Math.min(offset + chunkSize, file.size);
            const chunk = file.slice(offset, end);
            const buffer = await chunk.arrayBuffer();
            const text = decoder.decode(buffer, { stream: true });

            const joined = leftover + text;
            const lines = joined.split('\n');
            leftover = lines.pop() || '';

            for (const line of lines) {
                processLine(line);
            }

            offset = end;
            const now = Date.now();
            if (now - lastProgressTs > 250) {
                lastProgressTs = now;
                self.postMessage({
                    type: 'progress',
                    progress: Math.min(99, Math.round((offset / file.size) * 100)),
                    rowCount,
                });
            }
        }

        if (leftover.trim()) {
            processLine(leftover);
        }

        const topCustomerList = Array.from(topCustomers.entries())
            .map(([name, total]) => ({ name, total }))
            .sort((a, b) => b.total - a.total)
            .slice(0, 10);

        self.postMessage({
            type: 'done',
            progress: 100,
            metadata: {
                filename: file.name,
                size: file.size,
                lastModified: file.lastModified,
                rowCount,
                pricingTotal,
                billingTotal,
                invoiceCount: invoiceSet.size,
                topCustomers: topCustomerList,
                generatedAt: new Date().toISOString(),
            },
        });
    } catch (error) {
        self.postMessage({
            type: 'error',
            message: error?.message || 'Failed to process file.',
        });
    }
};
