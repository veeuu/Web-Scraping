const fs = require('fs/promises');
const path = require('path');
const axios = require('axios');
const cheerio = require('cheerio');
const puppeteer = require('puppeteer');
const pdf = require('pdf-parse');
const mammoth = require('mammoth');
const xlsx = require('xlsx');
const { exec } = require('child_process');

const SCRAPINGDOG_API_KEYS = [
];
const SEARCH_QUERY = '"vmware" or vCenter or vsphere or esxi'; // keyword search here 
const MAX_CRAWL_DEPTH = 3;
const MAX_KEYWORDS_PER_DOMAIN = 3;
const REQUEST_DELAY = 1500;

const WEBSITES_FILE = 'input file';  // input file name 
const KEYWORDS_FILE = 'keywords.json';  // keywords.json file
const RESULTS_DIR = 'json pages folder';  // json pages store folder
const RESULTS_CSV = 'output file';   // output file save 

let currentApiKeyIndex = 0;

/**
 * Pauses execution for a specified number of milliseconds.
 * @param {number} ms - The number of milliseconds to wait.
 */
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const ensureDirectoryExists = async (dirPath) => {
    try {
        await fs.access(dirPath);
    } catch (error) {
        if (error.code === 'ENOENT') {
            await fs.mkdir(dirPath, { recursive: true });
            console.log(`Created directory: ${dirPath}`);
        } else {
            throw error;
        }
    }
};

const getCanonicalDomain = async (domain) => {
    // This function now directly returns the domain without performing pre-flight checks.
    return domain;
};

const performGoogleSearch = async (domain) => {
    const searchQuery = `site:${domain} ${SEARCH_QUERY}`;
    
    while (currentApiKeyIndex < SCRAPINGDOG_API_KEYS.length) {
        const currentApiKey = SCRAPINGDOG_API_KEYS[currentApiKeyIndex];
        const url = `https://api.scrapingdog.com/google?api_key=${currentApiKey}&query=${encodeURIComponent(searchQuery)}`;
        
        console.log(`🔍 Searching for: "${searchQuery}" using key #${currentApiKeyIndex + 1}`);

        try {
            const response = await axios.get(url, { timeout: 30000 });
            if (response.data) {
                const filePath = path.join(RESULTS_DIR, `${domain}.json`);
                await fs.writeFile(filePath, JSON.stringify(response.data, null, 2));
                console.log(`Success with key #${currentApiKeyIndex + 1}. Saved results to: ${filePath}`);
                return response.data;
            }
            return null;
        } catch (error) {
            if (error.response && [401, 403].includes(error.response.status)) {
                console.warn(`-  Key #${currentApiKeyIndex + 1} failed (Status: ${error.response.status}). Trying next key...`);
                currentApiKeyIndex++;
            } else {
                console.error(` Error searching for domain ${domain}:`, error.message);
                return null;
            }
        }
    }
    console.error(` All ${SCRAPINGDOG_API_KEYS.length} API keys failed. Halting search for ${domain}.`);
    return null;
};

const extractUrls = (searchData) => {
    const results = searchData.organic_results || searchData.organic_data;
    if (!results || results.length === 0) {
        console.warn(' No organic results found in search data.');
        return [];
    }
    return results.map(result => result.link).filter(Boolean);
};

const searchKeywordsOnPage = (pageText, keywordsToSearch) => {
    if (!pageText) return [];
    const foundKeywords = new Set();
    const lowerCaseText = pageText.toLowerCase();
    for (const keyword of keywordsToSearch) {
        if (lowerCaseText.includes(keyword.toLowerCase())) {
            foundKeywords.add(keyword.toLowerCase());
        }
    }
    return Array.from(foundKeywords);
};

async function fetchAndParseContent(url, browser) {
    try {
        const response = await axios.get(url, {
            responseType: 'arraybuffer',
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36' },
            timeout: 20000
        });
        const contentType = response.headers['content-type'] || '';

        if (contentType.includes('application/pdf')) {
            console.log(`       ... Parsing PDF content from: ${url}`);
            const data = await pdf(response.data);
            return data.text;
        }
        if (contentType.includes('wordprocessingml')) {
            console.log(`       ... Parsing DOCX content from: ${url}`);
            const { value } = await mammoth.extractRawText({ buffer: response.data });
            return value;
        }
        if (contentType.includes('spreadsheetml')) {
            console.log(`       ... Parsing XLSX content from: ${url}`);
            const workbook = xlsx.read(response.data, { type: 'buffer' });
            let fullText = '';
            workbook.SheetNames.forEach(sheetName => {
                const sheet = workbook.Sheets[sheetName];
                fullText += xlsx.utils.sheet_to_txt(sheet) + '\n';
            });
            return fullText;
        }
        console.log(`       ... Parsing HTML content with Axios from: ${url}`);
        const html = response.data.toString('utf-8');
        const $ = cheerio.load(html);
        return $('body').text();
    } catch (error) {
        console.warn(`       ... Axios failed for ${url} (${error.message}). Retrying with Puppeteer for HTML.`);
        let page = null;
        try {
            page = await browser.newPage();
            await page.goto(url, { waitUntil: 'networkidle2', timeout: 45000 });
            const textContent = await page.evaluate(() => document.body.innerText);
            console.log(`       ... successfully fetched and parsed HTML with Puppeteer: ${url}`);
            return textContent;
        } catch (puppeteerError) {
            console.error(`       ...  Puppeteer also failed for ${url}:`, puppeteerError.message);
            return null;
        } finally {
            if (page) await page.close();
        }
    }
}

/**
 * Displays a live countdown in the console, then shuts down the computer.
 * @param {number} delayInSeconds - The number of seconds for the countdown.
 */
/*
const scheduleCountdownAndShutdown = async (delayInSeconds) => {
    console.log(`\n --- Task Complete. System will shut down shortly. ---`);
    console.log("To cancel the shutdown, press CTRL + C in this terminal window NOW.");

    // This loop creates the visible countdown in the terminal
    for (let i = delayInSeconds; i > 0; i--) {
        // \r moves the cursor to the start of the line, allowing us to overwrite it
        process.stdout.write(`\rShutting down in ${i} second(s)... `);
        await delay(1000); // Wait for one second
    }

    // Clear the countdown line and print the final message
    process.stdout.write('\rShutting down now...                 \n');

    let command;
    const platform = process.platform;

    if (platform === 'win32') {
        // Shutdown in 1 second. /s is for shutdown, /t is for time.
        command = `shutdown /s /t 1`;
    } else if (platform === 'linux' || platform === 'darwin') { // 'darwin' is the name for macOS
        // Shutdown immediately. -h is for halt.
        command = `sudo shutdown -h now`;
    } else {
        console.warn(`\n Automatic shutdown is not supported on this platform: ${platform}`);
        return;
    }

    exec(command, (error, stdout, stderr) => {
        if (error) {
            console.error(`\n Error initiating shutdown: ${error.message}`);
            console.error("Please ensure you have the necessary permissions (e.g., run as administrator or with 'sudo').");
            return;
        }
        if (stderr) {
            // Some systems output non-error messages to stderr, so we log it as a warning.
            console.warn(`\nShutdown command issued with a warning: ${stderr}`);
        }
        console.log(`\n Shutdown command successfully issued. ${stdout}`);
    });
};

*/
//
(async () => {
    await ensureDirectoryExists(RESULTS_DIR);


    try {
        await fs.access(RESULTS_CSV);
    } catch (error) {
        if (error.code === 'ENOENT') {
            console.log('Results CSV not found. Creating a new one.');
            await fs.writeFile(RESULTS_CSV, 'domain,keyword,reference_url\n');
        } else {
            throw error;
        }
    }

    const processedDomains = new Set();
    const existingResultsData = await fs.readFile(RESULTS_CSV, 'utf-8');
    existingResultsData.split(/\r?\n/).slice(1).forEach(line => {
        const domain = line.split(',')[0];
        if (domain) {
            processedDomains.add(domain.trim());
        }
    });

    console.log('--- Crawler Initialized ---');
    if (processedDomains.size > 0) {
        console.log(`Found ${processedDomains.size} domains already processed. They will be skipped.`);
    }

    const websitesData = await fs.readFile(WEBSITES_FILE, 'utf-8');
    const domains = websitesData.split(/\r?\n/).filter(line => line.trim() !== '');

    const keywordsData = await fs.readFile(KEYWORDS_FILE, 'utf-8');
    const allKeywords = JSON.parse(keywordsData).map(k => k.toLowerCase().trim());

    console.log(`Loaded ${domains.length} domains to process and ${allKeywords.length} unique lowercase keywords.`);

    for (const domain of domains) {
        if (processedDomains.has(domain)) {
            console.log(`\n- Skipping already processed domain: ${domain}`);
            continue;
        }

        console.log(`\n--- Processing Domain: ${domain} ---`);
        const visitedUrls = new Set();
        const foundKeywordsForDomain = new Set();
        let puppeteerBrowser = null;
        
        currentApiKeyIndex = 0;

        try {
            const canonicalDomain = await getCanonicalDomain(domain);
            const searchResults = await performGoogleSearch(canonicalDomain);
            if (!searchResults) {
                
                const csvRow = `${domain},,SEARCH FAILED\n`;
                await fs.appendFile(RESULTS_CSV, csvRow);
                continue;
            }

            const urlsToCrawl = extractUrls(searchResults);
            if (urlsToCrawl.length === 0) {
                console.log(`- Search was successful, but no crawlable URLs were found.`);
            }

            if (urlsToCrawl.length > 0) {
                console.log('--- Checking for keywords directly in result URLs ---');
                for (const url of urlsToCrawl) {
                    if (foundKeywordsForDomain.size >= MAX_KEYWORDS_PER_DOMAIN) break;
                    const lowerCaseUrl = url.toLowerCase();
                    const keywordsToSearchInUrl = allKeywords.filter(k => !foundKeywordsForDomain.has(k));
                    for (const keyword of keywordsToSearchInUrl) {
                        if (lowerCaseUrl.includes(keyword)) {
                            console.log(`   Found keyword '${keyword}' directly in URL: ${url}`);
                            foundKeywordsForDomain.add(keyword);
                            const csvRow = `${domain},${keyword},${url}\n`;
                            await fs.appendFile(RESULTS_CSV, csvRow);
                            if (foundKeywordsForDomain.size >= MAX_KEYWORDS_PER_DOMAIN) break;
                        }
                    }
                    if (foundKeywordsForDomain.size >= MAX_KEYWORDS_PER_DOMAIN) break;
                }
                console.log(`--- Finished URL check. Found ${foundKeywordsForDomain.size} keyword(s) so far. ---`);
            }

            if (foundKeywordsForDomain.size < MAX_KEYWORDS_PER_DOMAIN && urlsToCrawl.length > 0) {
                console.log('Launching Puppeteer instance for this domain...');
                puppeteerBrowser = await puppeteer.launch({
                    headless: true,
                    args: ['--no-sandbox', '--disable-setuid-sandbox']
                });
                const crawlQueue = urlsToCrawl.map(url => ({ url, depth: 1 }));

                while (crawlQueue.length > 0) {
                    if (foundKeywordsForDomain.size >= MAX_KEYWORDS_PER_DOMAIN) {
                        console.log(`Limit reached. Halting crawl.`);
                        break;
                    }
                    const { url, depth } = crawlQueue.shift();
                    if (visitedUrls.has(url) || depth > MAX_CRAWL_DEPTH) continue;
                    
                    console.log(`   Crawling (depth ${depth}): ${url}`);
                    visitedUrls.add(url);
                    
                    let pageText = await fetchAndParseContent(url, puppeteerBrowser);
                    
                    if (pageText) {
                        const keywordsToSearch = allKeywords.filter(k => !foundKeywordsForDomain.has(k));
                        if (keywordsToSearch.length > 0) {
                            const newlyFoundKeywords = searchKeywordsOnPage(pageText, keywordsToSearch);
                            if (newlyFoundKeywords.length > 0) {
                                console.log(`       Found new keywords: [${newlyFoundKeywords.join(', ')}] on ${url}`);
                                for (const k of newlyFoundKeywords) {
                                    if (!foundKeywordsForDomain.has(k)) {
                                        foundKeywordsForDomain.add(k);
                                        const csvRow = `${domain},${k},${url}\n`;
                                        await fs.appendFile(RESULTS_CSV, csvRow);
                                        if (foundKeywordsForDomain.size >= MAX_KEYWORDS_PER_DOMAIN) {
                                            console.log(`          Limit reached.`);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    await delay(REQUEST_DELAY);
                }
            }
        } catch (error) {
            console.error(` An unexpected error occurred while processing ${domain}:`, error);
        } finally {
            console.log(`--- Cleaning up resources for ${domain} ---`);
            if (puppeteerBrowser) {
                console.log(' Closing Puppeteer instance...');
                await puppeteerBrowser.close();
            }
        }
        
        if (foundKeywordsForDomain.size === 0) {
            console.log(`- No keywords found for ${domain} after all checks. Recording in CSV to prevent re-processing.`);
            const csvRow = `${domain},,NOT FOUND\n`;
            await fs.appendFile(RESULTS_CSV, csvRow);
        }
    }

    console.log('\n--- All domains processed. Script finished. ---');


})();