const querystring = require('querystring');
const { Driver, getCredentialsFromEnv } = require('ydb-sdk');
const https = require('https');

// Глобальные переменные для connection pooling
let driverInstance = null;
let driverInitializing = false;
let lastDriverError = null;
let healthCheckInterval = null;

const YDB_CONFIG = {
    endpoint: 'ydb.serverless.yandexcloud.net:2135',
    database: '/ru-central1/b1gr4oube9qtanehd3eg/etnagrf3t4vkvb7r3o03',
    connectionTimeout: 10000,
    queryTimeout: 30000
};

const HEALTH_CHECK_INTERVAL = 30000;

// ====================== УЛУЧШЕННОЕ ПОДКЛЮЧЕНИЕ К YDB ======================

async function initYDBDriverWithRetry() {
    if (driverInstance && await isDriverReady()) {
        return driverInstance;
    }

    if (driverInitializing) {
        console.log('### [DEBUG] Waiting for existing driver initialization...');
        await new Promise(resolve => setTimeout(resolve, 1000));
        return initYDBDriverWithRetry();
    }

    driverInitializing = true;
    console.log('### [DEBUG] Initializing YDB driver with retry...');

    const maxRetries = 5;
    const initialDelay = 1000;
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            console.log(`### [DEBUG] YDB connection attempt ${attempt}/${maxRetries}`);
            
            const driver = new Driver({
                endpoint: YDB_CONFIG.endpoint,
                database: YDB_CONFIG.database,
                authService: getCredentialsFromEnv(),
                connectionTimeout: YDB_CONFIG.connectionTimeout
            });

            const readyPromise = driver.ready(YDB_CONFIG.connectionTimeout);
            const timeoutPromise = new Promise((_, reject) => 
                setTimeout(() => reject(new Error('Driver ready timeout')), YDB_CONFIG.connectionTimeout + 2000)
            );

            await Promise.race([readyPromise, timeoutPromise]);

            console.log('### [DEBUG] YDB driver initialized successfully');
            driverInstance = driver;
            lastDriverError = null;
            
            startHealthChecks();
            
            return driverInstance;

        } catch (error) {
            lastDriverError = error;
            console.error(`### [ERROR] YDB driver initialization attempt ${attempt} failed:`, error.message);

            if (attempt === maxRetries) {
                console.error('### [ERROR] All YDB connection attempts failed');
                driverInitializing = false;
                throw error;
            }

            const delay = initialDelay * Math.pow(2, attempt - 1);
            console.log(`### [DEBUG] Retrying in ${delay}ms...`);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }

    driverInitializing = false;
    throw new Error('Failed to initialize YDB driver after retries');
}

async function getYDBDriver() {
    try {
        if (driverInstance && await isDriverReady()) {
            return driverInstance;
        }
        
        return await initYDBDriverWithRetry();
    } catch (error) {
        console.error('### [ERROR] Failed to get YDB driver:', error.message);
        throw new Error('Database connection failed');
    }
}

async function isDriverReady() {
    if (!driverInstance) return false;
    
    try {
        return await driverInstance.ready(1000);
    } catch (error) {
        console.warn('### [WARN] Driver readiness check failed:', error.message);
        driverInstance = null;
        return false;
    }
}

function startHealthChecks() {
    if (healthCheckInterval) {
        clearInterval(healthCheckInterval);
    }
    
    healthCheckInterval = setInterval(async () => {
        try {
            if (driverInstance) {
                const isReady = await driverInstance.ready(2000);
                if (!isReady) {
                    console.warn('### [HEALTH] Driver not ready, resetting connection');
                    driverInstance = null;
                }
            }
        } catch (error) {
            console.warn('### [HEALTH] Health check failed:', error.message);
            driverInstance = null;
        }
    }, HEALTH_CHECK_INTERVAL);
}

// ====================== ОСНОВНОЙ HANDLER ======================

exports.handler = async (event) => {
    const responseConfig = {
        headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization'
        }
    };

    if (event.httpMethod === 'OPTIONS') {
        return {
            ...responseConfig,
            statusCode: 204,
            body: ''
        };
    }

    console.log('### [DEBUG] Incoming request:', JSON.stringify({
        httpMethod: event.httpMethod,
        path: event.path,
        queryParams: event.queryStringParameters,
        hasBody: !!event.body
    }, null, 2));

    try {
        const driver = await getYDBDriver().catch(error => {
            console.error('### [CRITICAL] YDB connection completely failed:', error);
            throw new Error('Database service unavailable');
        });

        let phone, source, verificationKey, parsedData = {};
        
        verificationKey = extractVerificationKey(event);
        console.log('### [DEBUG] Extracted verification key:', verificationKey);

        try {
            if (event.httpMethod === 'GET') {
                phone = event.queryStringParameters?.phone;
                source = 'telegram';
            } else {
                const bodyString = event.isBase64Encoded 
                    ? Buffer.from(event.body, 'base64').toString('utf-8') 
                    : event.body;
                const contentType = event.headers['content-type'] || event.headers['Content-Type'];
                
                if (!contentType) console.warn('### [WARNING] No content-type header provided');

                parsedData = contentType?.includes('application/json') 
                    ? JSON.parse(bodyString) 
                    : querystring.parse(bodyString);
                
                phone = parsedData.Phone || parsedData.phone;
                source = determineSource(parsedData, event.headers);
            }
        } catch (parseError) {
            console.error('### [ERROR] Request parsing failed:', parseError);
            throw new Error('Invalid request data format');
        }

        const normalizedPhone = normalizePhone(phone);
        if (!normalizedPhone || !isValidPhone(normalizedPhone)) {
            console.error('### [ERROR] Invalid phone format:', phone);
            throw new Error('Invalid phone format');
        }
        console.log(`### [DEBUG] Processing ${source} request for phone: ${normalizedPhone}`);

        if (source === 'telegram' || source === 'whatsapp') {
            return await handleVerification({
                driver,
                normalizedPhone,
                source,
                verificationKey,
                responseConfig
            });
        }

        if (source === 'tilda') {
            return await handleTildaSubmission({
                driver,
                event,
                parsedData,
                normalizedPhone,
                verificationKey,
                responseConfig
            });
        }

        throw new Error(`Unsupported source: ${source}`);

    } catch (error) {
        console.error('### [ERROR] Handler error:', {
            message: error.message,
            stack: error.stack
        });
        
        if (error.message.includes('Database connection failed') || 
            error.message.includes('UNAVAILABLE') ||
            error.message.includes('Name resolution failed') ||
            error.message.includes('Database service unavailable')) {
            return {
                ...responseConfig,
                statusCode: 503,
                body: JSON.stringify({
                    status: 'error',
                    message: 'Service temporarily unavailable',
                    retryable: true,
                    code: 'SERVICE_UNAVAILABLE'
                })
            };
        }
        
        return {
            ...responseConfig,
            statusCode: 200,
            body: JSON.stringify({
                status: 'error',
                message: error.message || 'Request processing error',
                code: 'PROCESSING_ERROR'
            })
        };
    }
};

// ====================== Вспомогательные функции ======================

function extractVerificationKey(event) {
    let key = null;
    
    if (event.queryStringParameters?.key) {
        key = event.queryStringParameters.key;
    } 
    else if (event.multiValueQueryStringParameters?.key) {
        key = event.multiValueQueryStringParameters.key[0];
    }
    else if (event.rawQuery) {
        try {
            const params = new URLSearchParams(event.rawQuery);
            key = params.get('key');
        } catch (e) {
            console.error('### [ERROR] Failed to parse rawQuery:', e);
        }
    }
    
    if (!key) {
        const webhookUrl = event.headers?.['x-webhook-url'] || event.headers?.['X-Webhook-Url'];
        if (webhookUrl) {
            try {
                const urlObj = new URL(webhookUrl);
                key = urlObj.searchParams.get('key');
            } catch (e) {
                console.error('### [ERROR] Error parsing webhook URL:', e);
            }
        }
    }
    
    if (key && !/^[a-f0-9]{32}$/.test(key)) {
        console.warn('### [WARNING] Invalid verification key format:', key);
        return null;
    }
    
    console.log('### [DEBUG] Extracted verification key:', key);
    return key;
}

function normalizePhone(phone) {
    if (!phone) return null;
    
    try {
        let cleanPhone = phone.toString().replace(/[^\d+]/g, '');
        
        if (cleanPhone.startsWith('+')) return cleanPhone;
        if (cleanPhone.startsWith('8') && cleanPhone.length === 11) return '+7' + cleanPhone.substring(1);
        if (cleanPhone.startsWith('7') && cleanPhone.length === 11) return '+' + cleanPhone;
        if (cleanPhone.length === 10) return '+7' + cleanPhone;
        
        return '+' + cleanPhone;
    } catch (e) {
        console.error('### [ERROR] Phone normalization failed:', e);
        return null;
    }
}

function isValidPhone(phone) {
    if (!phone) return false;
    return /^\+[0-9]{10,15}$/.test(phone);
}

function determineSource(data, headers) {
    if (!data) return 'unknown';
    
    if (data.source) return data.source.toString().toLowerCase();
    if (data.call_status !== undefined || data.call_duration !== undefined || data.wa_verified !== undefined) return 'whatsapp';
    if (data.formid || data.Name || data.Phone) return 'tilda';
    
    const userAgent = headers?.['user-agent'] || headers?.['User-Agent'];
    if (userAgent?.includes('Apache-HttpClient')) return 'whatsapp';
    
    return 'unknown';
}

// ====================== ОБРАБОТКА ВЕРИФИКАЦИИ (Telegram/WhatsApp) ======================

async function handleVerification({ driver, normalizedPhone, source, verificationKey, responseConfig }) {
    const attemptId = Date.now().toString();
    console.log('### [DEBUG] Starting verification process:', { 
        attemptId, 
        phone: normalizedPhone, 
        source 
    });

    // 1. Поиск оригинальной заявки в новой таблице
    const originalData = await findOriginalRawSubmission(driver, normalizedPhone);
    console.log('### [DEBUG] Original submission data found:', !!originalData);

    const foundInSubmissions = !!originalData;

    // 2. Проверка временного окна (5 минут)
    if (foundInSubmissions) {
        const submissionTime = new Date(originalData.timestamp);
        const currentTime = new Date();
        const timeDiffMinutes = (currentTime - submissionTime) / (1000 * 60);

        if (timeDiffMinutes > 5) {
            await logVerificationAttempt({
                driver,
                attemptId,
                normalizedPhone,
                source,
                verified: false,
                foundInSubmissions: true,
                status: 'timeout'
            });

            return {
                ...responseConfig,
                statusCode: 200,
                body: JSON.stringify({
                    status: 'timeout',
                    message: 'Verification window expired',
                    phone: normalizedPhone,
                    verified: false
                })
            };
        }
    }

    // 3. Логирование попытки верификации
    await logVerificationAttempt({
        driver,
        attemptId,
        normalizedPhone,
        source,
        verified: false,
        foundInSubmissions
    });

    if (!foundInSubmissions) {
        return {
            ...responseConfig,
            statusCode: 200,
            body: JSON.stringify({
                status: 'success',
                message: 'Phone not found in forms',
                phone: normalizedPhone,
                verified: false
            })
        };
    }

    // 4. Определение конечного ключа верификации
    const finalVerificationKey = verificationKey || originalData.verification_key;
    console.log('### [DEBUG] Using verification key:', finalVerificationKey);

    // 5. Получение URL вебхука
    const webhookUrl = await getWebhookEndpoint(driver, finalVerificationKey);
    if (!webhookUrl) {
        throw new Error('Webhook endpoint not found');
    }

    // 6. Отправка ВСЕХ исходных данных ВКЛЮЧАЯ КУКИ (ОДИН запрос)
    try {
        // Формируем данные для вебхука - отправляем ВСЕ исходные данные включая куки
        const webhookData = {
            ...originalData.raw_data,  // Все оригинальные данные из Тильды (включая COOKIES)
            verification_phone: normalizedPhone, // Добавляем номер для верификации
            verification_source: source, // Источник верификации
            verification_timestamp: new Date().toISOString(),
            verified: true
        };

        console.log('### [DEBUG] Prepared webhook data with cookies:', JSON.stringify(webhookData, null, 2));

        // 6.1. Отправляем данные в вебхук с куками в заголовке
        await sendToWebhookWithCookies(webhookUrl, webhookData, originalData.raw_data.COOKIES);
        
        // 7. Обновление статуса в базе
        await executeYdbQuery(driver, `
            UPDATE raw_submissions 
            SET phone_verified = true,
                webhook_sent = true,
                verification_key = "${escapeString(finalVerificationKey)}"
            WHERE phone = "${escapeString(normalizedPhone)}"
        `);

        return {
            ...responseConfig,
            statusCode: 200,
            body: JSON.stringify({
                status: 'success',
                message: 'Phone verified successfully',
                phone: normalizedPhone,
                verified: true,
                webhook_sent: true,
                cookies_included: !!originalData.raw_data.COOKIES
            })
        };

    } catch (error) {
        console.error('### [ERROR] Webhook failed:', error);
        
        await executeYdbQuery(driver, `
            UPDATE raw_submissions 
            SET webhook_sent = false
            WHERE phone = "${escapeString(normalizedPhone)}"
        `);

        return {
            ...responseConfig,
            statusCode: 200,
            body: JSON.stringify({
                status: 'error',
                message: 'Verification completed but webhook failed',
                phone: normalizedPhone,
                verified: true,
                webhook_sent: false,
                error: error.message
            })
        };
    }
}

// ====================== ОБНОВЛЕННАЯ ФУНКЦИЯ ДЛЯ ОТПРАВКИ С КУКАМИ ======================

async function sendToWebhookWithCookies(url, data, cookies) {
    if (!url) throw new Error('Webhook URL is required');
    
    console.log('### [DEBUG] Preparing single webhook request with cookies:', {
        url: url,
        hasCookies: !!cookies,
        dataKeys: Object.keys(data)
    });
    
    const postData = JSON.stringify(data);
    const options = {
        method: 'POST',
        headers: { 
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(postData)
        },
        timeout: 10000
    };
    
    // Добавляем куки в заголовок, если они есть
    if (cookies) {
        options.headers['Cookie'] = cookies;
        console.log('### [DEBUG] Added cookies to request header');
    }
    
    return new Promise((resolve, reject) => {
        const req = https.request(url, options, (res) => {
            let responseData = '';
            
            res.on('data', chunk => {
                responseData += chunk;
            });
            
            res.on('end', () => {
                console.log('### [DEBUG] Webhook response:', {
                    statusCode: res.statusCode,
                    headers: res.headers
                });
                
                if (res.statusCode >= 200 && res.statusCode < 300) {
                    resolve(responseData);
                } else {
                    const error = new Error(`Webhook responded with status ${res.statusCode}`);
                    error.response = {
                        status: res.statusCode,
                        data: responseData
                    };
                    reject(error);
                }
            });
        });
        
        req.on('error', error => {
            console.error('### [ERROR] Webhook request failed:', error);
            reject(error);
        });
        
        req.on('timeout', () => {
            console.error('### [ERROR] Webhook request timed out');
            req.destroy(new Error('Request timeout'));
        });
        
        req.write(postData);
        req.end();
    });
}

// ====================== ОБНОВЛЕННАЯ ФУНКЦИЯ sendToWebhook ======================

// async function sendToWebhook(url, data) {
//     if (!url) throw new Error('Webhook URL is required');
    
//     console.log('### [DEBUG] Preparing webhook request:', {
//         url: url,
//         data: data
//     });
    
//     const postData = JSON.stringify(data);
//     const options = {
//         method: 'POST',
//         headers: { 
//             'Content-Type': 'application/json',
//             'Content-Length': Buffer.byteLength(postData)
//         },
//         timeout: 10000
//     };
    
//     return new Promise((resolve, reject) => {
//         const req = https.request(url, options, (res) => {
//             let responseData = '';
            
//             res.on('data', chunk => {
//                 responseData += chunk;
//             });
            
//             res.on('end', () => {
//                 console.log('### [DEBUG] Webhook response:', {
//                     statusCode: res.statusCode,
//                     headers: res.headers,
//                     body: responseData
//                 });
                
//                 if (res.statusCode >= 200 && res.statusCode < 300) {
//                     resolve(responseData);
//                 } else {
//                     const error = new Error(`Webhook responded with status ${res.statusCode}`);
//                     error.response = {
//                         status: res.statusCode,
//                         data: responseData
//                     };
//                     reject(error);
//                 }
//             });
//         });
        
//         req.on('error', error => {
//             console.error('### [ERROR] Webhook request failed:', error);
//             reject(error);
//         });
        
//         req.on('timeout', () => {
//             console.error('### [ERROR] Webhook request timed out');
//             req.destroy(new Error('Request timeout'));
//         });
        
//         req.write(postData);
//         req.end();
//     });
// }

// ====================== ОБРАБОТКА ТИЛЬДЫ (сохранение всего запроса) ======================

async function handleTildaSubmission({ driver, event, parsedData, normalizedPhone, verificationKey, responseConfig }) {
    const submissionId = Date.now().toString();
    const domain = extractDomainFromReferer(event.headers) || 'tilda';
    
    console.log('### [DEBUG] Processing Tilda submission:', {
        submissionId,
        phone: normalizedPhone,
        verificationKey,
        parsedData: JSON.stringify(parsedData)
    });
    
    // Сохраняем ВЕСЬ запрос в поле raw_data
    const query = `
        INSERT INTO raw_submissions (
            id, timestamp, phone, source,
            raw_data, verification_key,
            phone_verified, webhook_sent
        ) VALUES (
            "${submissionId}",
            CurrentUtcTimestamp(),
            "${escapeString(normalizedPhone)}",
            "${escapeString(domain)}",
            '${JSON.stringify(parsedData).replace(/'/g, "''")}',
            ${verificationKey ? `"${escapeString(verificationKey)}"` : 'NULL'},
            false,
            false
        )
    `;
    
    console.log('### [DEBUG] Executing INSERT query for raw data');
    
    try {
        await executeYdbQuery(driver, query);
        
        return {
            ...responseConfig,
            statusCode: 200,
            body: JSON.stringify({
                status: 'success',
                message: 'Form data saved successfully',
                submissionId,
                phone: normalizedPhone,
                verificationKey: verificationKey || null
            })
        };
    } catch (error) {
        console.error('### [ERROR] Tilda submission failed:', error);
        throw error;
    }
}

// ====================== НОВЫЕ ФУНКЦИИ ДЛЯ РАБОТЫ С RAW DATA ======================

async function findOriginalRawSubmission(driver, phone) {
    if (!phone) {
        console.error('### [ERROR] Phone number is required for search');
        return null;
    }
    
    const query = `
        SELECT 
            raw_data,
            verification_key,
            timestamp
        FROM raw_submissions 
        WHERE phone = "${escapeString(phone)}"
        ORDER BY timestamp DESC
        LIMIT 1
    `;
    
    console.log('### [DEBUG] Executing raw submission query:', query);
    
    try {
        const result = await driver.tableClient.withSession(session => 
            session.executeQuery(query)
        );
        
        if (!result.resultSets?.[0]?.rows?.[0]) {
            console.log('### [DEBUG] No original submission found for phone:', phone);
            return null;
        }
        
        const row = result.resultSets[0].rows[0];
        
        // Парсим JSON данные
        let rawData = {};
        try {
            const rawDataString = row.items[0].textValue || row.items[0].bytesValue?.toString();
            if (rawDataString) {
                rawData = JSON.parse(rawDataString);
            }
        } catch (e) {
            console.error('### [ERROR] Failed to parse raw_data:', e);
        }
        
        return {
            raw_data: rawData,
            verification_key: row.items[1]?.textValue || row.items[1]?.bytesValue?.toString(),
            timestamp: new Date(row.items[2].uint64Value?.toString() || Date.now())
        };
    } catch (error) {
        console.error('### [ERROR] Failed to find original submission:', error);
        throw error;
    }
}

// ====================== СУЩЕСТВУЮЩИЕ ФУНКЦИИ ======================

async function getWebhookEndpoint(driver, key) {
    if (!key) {
        console.error('### [ERROR] No verification key provided');
        return null;
    }
    
    const query = `
        SELECT endpoint_url, enabled
        FROM webhook_endpoints 
        WHERE key = "${escapeString(key)}"
        LIMIT 1
    `;
    
    console.log('### [DEBUG] Executing webhook endpoint query:', query);
    
    try {
        const result = await driver.tableClient.withSession(session => 
            session.executeQuery(query)
        );
        
        if (!result.resultSets?.[0]?.rows?.[0]) {
            console.error('### [ERROR] No webhook endpoint found for key:', key);
            return null;
        }
        
        const row = result.resultSets[0].rows[0];
        
        let endpoint, enabled;
        
        if (row.items[0].textValue !== undefined) {
            endpoint = row.items[0].textValue;
        } 
        else if (row.items[0].bytesValue !== undefined) {
            endpoint = row.items[0].bytesValue.toString('utf-8');
        } 
        else {
            console.error('### [ERROR] Unknown endpoint_url format:', row.items[0]);
            return null;
        }
        
        enabled = row.items[1]?.boolValue ?? false;
        
        console.log('### [DEBUG] Found webhook endpoint:', {
            endpoint,
            enabled,
            key
        });
        
        if (!enabled) {
            console.error('### [ERROR] Webhook endpoint disabled for key:', key);
            return null;
        }
        
        return endpoint;
    } catch (error) {
        console.error('### [ERROR] Failed to get webhook endpoint:', error);
        throw error;
    }
}

async function logVerificationAttempt({ driver, attemptId, normalizedPhone, source, verified, foundInSubmissions, status }) {
    const query = `
        INSERT INTO incoming_verification_attempts (
            id, timestamp, phone, source,
            verified, found_in_submissions
        ) VALUES (
            "${attemptId}",
            CurrentUtcTimestamp(),
            "${escapeString(normalizedPhone)}",
            "${escapeString(source)}",
            ${verified},
            ${foundInSubmissions}
        )
    `;
    
    console.log('### [DEBUG] Logging verification attempt:', query);
    
    try {
        await executeYdbQuery(driver, query);
    } catch (error) {
        console.error('### [ERROR] Failed to log verification attempt:', error);
        throw error;
    }
}

function extractDomainFromReferer(headers) {
    const referer = headers?.referer || headers?.Referer;
    if (!referer) {
        console.log('### [DEBUG] No referer header found');
        return null;
    }
    
    try {
        const domain = new URL(referer).hostname.replace('www.', '');
        console.log('### [DEBUG] Extracted domain from referer:', domain);
        return domain;
    } catch (e) {
        console.error('### [ERROR] Failed to parse referer:', referer);
        return null;
    }
}

function escapeString(str) {
    if (str === null || str === undefined) return null;
    
    try {
        return str.toString()
            .replace(/\\/g, '\\\\')
            .replace(/"/g, '\\"')
            .replace(/'/g, "\\'")
            .replace(/\n/g, '\\n')
            .replace(/\r/g, '\\r');
    } catch (e) {
        console.error('### [ERROR] String escaping failed:', e);
        return null;
    }
}

async function executeYdbQuery(driver, query) {
    console.log('### [DEBUG] Executing YDB query:', query);
    
    try {
        const result = await driver.tableClient.withSession(session => 
            session.executeQuery(query)
        );
        console.log('### [DEBUG] Query executed successfully');
        return result;
    } catch (error) {
        console.error('### [ERROR] Query execution failed:', {
            query: query,
            error: error.message,
            stack: error.stack
        });
        throw error;
    }
}

// Graceful shutdown
process.on('SIGTERM', () => {
    if (healthCheckInterval) {
        clearInterval(healthCheckInterval);
    }
    if (driverInstance) {
        driverInstance.destroy();
    }
});
