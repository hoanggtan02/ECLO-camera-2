<?php

// File: persistent_listener.php
// Chạy file này bằng Supervisor hoặc Docker để đảm bảo hoạt động 24/7

error_log("Khởi động MQTT Listener...");
error_log("Script is running as user: " . get_current_user());

require __DIR__ . '/../../vendor/autoload.php';

use PhpMqtt\Client\MqttClient;
use PhpMqtt\Client\ConnectionSettings;
use Medoo\Medoo;
use Predis\Client as RedisClient;

// ===================================================================
// == HELPER FUNCTIONS (CÁC HÀM HỖ TRỢ) ==
// ===================================================================

/**
 * Gửi một tin nhắn đến MQTT Broker.
 * @param array $env Mảng chứa cấu hình từ file .env
 * @param string $topic Topic để gửi tin nhắn
 * @param array $payload Mảng dữ liệu của tin nhắn
 * @return bool True nếu thành công, False nếu thất bại
 */
function publishMqttMessage(array $env, string $topic, array $payload): bool
{
    $mqttServer = $env['MQTT_HOST'] ?? 'mqtt.ellm.io';
    $mqttPort = (int)($env['MQTT_PORT_TCP'] ?? 1883);
    $mqttUsername = $env['MQTT_USERNAME'] ?? 'eclo';
    $mqttPassword = $env['MQTT_PASSWORD'] ?? 'Eclo@123';
    $mqttClientId = 'backend-publisher-' . uniqid();

    try {
        $mqtt = new MqttClient($mqttServer, $mqttPort, $mqttClientId);
        $connectionSettings = (new ConnectionSettings)
            ->setUsername($mqttUsername)
            ->setPassword($mqttPassword)
            ->setConnectTimeout(5);
        $mqtt->connect($connectionSettings, true);
        $mqtt->publish($topic, json_encode($payload, JSON_UNESCAPED_UNICODE), 0);
        $mqtt->disconnect();
        error_log("✅ MQTT Publish Success to topic [{$topic}]");
        return true;
    } catch (Exception $e) {
        error_log("❌ MQTT Publish Error: " . $e->getMessage());
        return false;
    }
}

/**
 * Lưu hình ảnh từ chuỗi base64 và trả về mảng đường dẫn.
 * @return array|null Mảng chứa ['faces_path', 'photos_path'] hoặc null nếu thất bại.
 */
function save_image_from_base64(?string $picBase64, string $facesUploadPath, string $photosUploadPath, string $prefix, string $uniqueId): ?array
{
    if (empty($picBase64)) {
        error_log("⚠️ [save_image] Chuỗi base64 rỗng.");
        return null;
    }

    // Log một phần chuỗi base64 để debug (giới hạn để tránh log quá dài)
    error_log("📷 [save_image] Chuỗi base64 đầu vào (50 ký tự đầu): " . substr($picBase64, 0, 50));

    if (!preg_match('/^data:image\/(jpeg|jpg|png);base64,/', $picBase64, $matches)) {
        error_log("⚠️ [save_image] Chuỗi base64 không đúng định dạng (không phải jpeg/jpg/png).");
        return null;
    }

    // Kiểm tra quyền thư mục
    if (!is_writable($facesUploadPath)) {
        error_log("⚠️ [save_image] Thư mục không thể ghi: $facesUploadPath");
        return null;
    }
    if (!is_writable($photosUploadPath)) {
        error_log("⚠️ [save_image] Thư mục không thể ghi: $photosUploadPath");
        return null;
    }

    try {
        list(, $data) = explode(',', $picBase64);
        $imageData = base64_decode($data);
        if ($imageData === false) {
            error_log("⚠️ [save_image] Lỗi giải mã base64: Chuỗi không hợp lệ.");
            return null;
        }

        $imageExtension = ($matches[1] === 'jpeg') ? 'jpg' : $matches[1];
        $imageName = $prefix . str_replace('.', '_', $uniqueId) . '_' . time() . '.' . $imageExtension;
        
        $facesFilePath = rtrim($facesUploadPath, '/') . '/' . $imageName;
        $photosFilePath = rtrim($photosUploadPath, '/') . '/' . $imageName;

        error_log("📁 [save_image] Đang lưu ảnh vào: $facesFilePath");

        if (file_put_contents($facesFilePath, $imageData) === false) {
            error_log("⚠️ [save_image] Không thể ghi file vào: $facesFilePath. Lỗi: " . error_get_last()['message']);
            return null;
        }
        if (!file_exists($facesFilePath)) {
            error_log("⚠️ [save_image] File không tồn tại sau khi ghi: $facesFilePath");
            return null;
        }
        chmod($facesFilePath, 0644);

        error_log("📁 [save_image] Đang sao chép ảnh sang: $photosFilePath");

        if (!copy($facesFilePath, $photosFilePath)) {
            error_log("⚠️ [save_image] Không thể sao chép file sang: $photosFilePath. Lỗi: " . error_get_last()['message']);
            unlink($facesFilePath);
            return null;
        }
        if (!file_exists($photosFilePath)) {
            error_log("⚠️ [save_image] File không tồn tại sau khi sao chép: $photosFilePath");
            unlink($facesFilePath);
            return null;
        }
        chmod($photosFilePath, 0644);
        
        error_log("✅ [save_image] Đã lưu và sao chép ảnh thành công: $imageName");

        return [
            'faces_path' => 'uploads/faces/' . $imageName,
            'photos_path' => 'uploads/photos/' . $imageName
        ];

    } catch (Exception $e) {
        error_log("⚠️ [save_image] Lỗi ngoại lệ khi xử lý ảnh: " . $e->getMessage());
        return null;
    }
}

// --- 1. TẢI CẤU HÌNH VÀ KHỞI TẠO KẾT NỐI ---

$envPath = __DIR__ . '/../../.env';
if (!file_exists($envPath)) die("FATAL ERROR: File .env không được tìm thấy.");
$env = parse_ini_file($envPath);
$publicBaseUrl = rtrim($env['APP_URL'] ?? 'http://localhost', '/');
$facesUploadPath = __DIR__ . '/../../public/uploads/faces/';
$photosUploadPath = __DIR__ . '/../../public/uploads/photos/';

// Log đường dẫn thư mục để debug
error_log("📁 [INIT] Đường dẫn thư mục faces: $facesUploadPath");
error_log("📁 [INIT] Đường dẫn thư mục photos: $photosUploadPath");

// Kiểm tra và tạo thư mục ngay từ đầu để báo lỗi sớm
foreach ([$facesUploadPath, $photosUploadPath] as $path) {
    if (!is_dir($path)) {
        if (!mkdir($path, 0775, true)) {
            die("FATAL ERROR: Không thể tạo thư mục {$path}.");
        }
        error_log("✅ [INIT] Đã tạo thư mục: $path");
    }
    if (!is_writable($path)) {
        die("FATAL ERROR: Thư mục {$path} không có quyền ghi. Vui lòng kiểm tra quyền của user đang chạy script (ví dụ: www-data).");
    }
    // Log quyền thư mục
    error_log("✅ [INIT] Quyền thư mục {$path}: " . substr(sprintf('%o', fileperms($path)), -4));
}

// Kết nối Database
try {
    $database = new Medoo([
        'database_type' => $env['DB_CONNECTION'] ?? 'mysql',
        'database_name' => $env['DB_DATABASE'] ?? 'eclo-camera',
        'server'        => $env['DB_HOST'] ?? 'localhost',
        'username'      => $env['DB_USERNAME'] ?? 'root',
        'password'      => $env['DB_PASSWORD'] ?? '',
        'charset'       => $env['DB_CHARSET'] ?? 'utf8mb4',
        'error'         => PDO::ERRMODE_EXCEPTION,
    ]);
    echo "✅ [LISTENER] Đã kết nối Database thành công.\n";
} catch (PDOException $e) {
    die("FATAL ERROR: Không thể kết nối đến database: " . $e->getMessage());
}

// Kết nối Redis
try {
    $redis = new RedisClient([
        'scheme' => 'tcp',
        'host'   => $env['REDIS_HOST'] ?? '127.0.0.1',
        'port'   => (int)($env['REDIS_PORT'] ?? 6379),
    ]);
    $redis->ping();
    echo "✅ [LISTENER] Đã kết nối Redis thành công.\n";
} catch (Exception $e) {
    die("FATAL ERROR: Không thể kết nối đến Redis: " . $e->getMessage());
}

// --- 2. CẤU HÌNH VÀ CHẠY MQTT LISTENER ---

$server        = $env['MQTT_HOST'] ?? 'mqtt.ellm.io';
$port          = (int)($env['MQTT_PORT_TCP'] ?? 1883);
$clientId      = 'backend-listener-' . uniqid();
$username      = $env['MQTT_USERNAME'] ?? 'eclo';
$password      = $env['MQTT_PASSWORD'] ?? 'Eclo@123';
$wildcardTopic = 'mqtt/face/1018656/+';

$mqtt = new MqttClient($server, $port, $clientId);
$connectionSettings = (new ConnectionSettings)->setUsername($username)->setPassword($password);

try {
    $mqtt->connect($connectionSettings, true);
    
    $mqtt->subscribe($wildcardTopic, function ($topic, $message) use ($database, $redis, $env, $facesUploadPath, $photosUploadPath, $publicBaseUrl) {
        
        echo "📨 [LISTENER] Nhận được tin nhắn trên topic [{$topic}] tại: " . microtime(true) . "\n";
        $payload = json_decode($message, true);
        if (!$payload || !isset($payload['info'])) {
            error_log("❌ [LISTENER] Payload không hợp lệ hoặc thiếu info: " . $message);
            return;
        }
        
        $info = $payload['info'];
        $eventType = basename($topic);
        error_log("📸 [DEBUG] Payload cho topic [$topic]: " . json_encode($payload, JSON_UNESCAPED_UNICODE));

        if ($eventType === 'Rec') {
            $imagePaths = save_image_from_base64($info['pic'] ?? null, $facesUploadPath, $photosUploadPath, 'rec_', $info['personId'] ?? uniqid());
            
            $recData = [
                'event_type'  => 'Rec',
                'person_name' => $info['personName'] ?? ($info['persionName'] ?? 'N/A'),
                'person_id'   => $info['personId'] ?? 'N/A',
                'similarity'  => (float)($info['similarity1'] ?? 0),
                'record_id'   => (int)($info['RecordID'] ?? 0),
                'event_time'  => $info['time'] ?? date('Y-m-d H:i:s'),
                'image_path'  => $imagePaths['faces_path'] ?? null,
            ];
            $database->insert('mqtt_messages', $recData);
            echo "✅ [REC] Đã ghi nhận sự kiện cho: " . $recData['person_name'] . "\n";
        }
        
        elseif ($eventType === 'Snap') {
            $picBase64 = $info['pic'] ?? null;
            
            // Chuẩn bị dữ liệu cơ bản cho mqtt_messages
            $snapData = [
                'event_type'  => 'Snap',
                'person_name' => 'Người lạ',
                'event_time'  => $info['time'] ?? date('Y-m-d H:i:s'),
                'image_path'  => null,
            ];

            if (!$picBase64) {
                error_log("❌ [SNAP] Payload thiếu pic base64.");
                $database->insert('mqtt_messages', $snapData);
                echo "✅ [SNAP] Đã ghi sự kiện Snap thiếu ảnh vào logs.\n";
                return;
            }

            // Sử dụng khóa phân tán để tránh race condition
            $imageHash = md5($picBase64);
            $redisKey = 'snap_cooldown:' . $imageHash;
            $lockKey = 'snap_lock:' . $imageHash;
            $lockAcquired = $redis->set($lockKey, 1, ['NX', 'EX' => 10]); // Khóa 10 giây

            if (!$lockAcquired) {
                error_log("ℹ️ [SNAP] Bỏ qua vì tin nhắn đang được xử lý bởi luồng khác.");
                $database->insert('mqtt_messages', $snapData);
                echo "✅ [SNAP] Đã ghi sự kiện Snap bị khóa vào logs.\n";
                return;
            }

            try {
                // Kiểm tra Redis khóa chống trùng lặp
                if ($redis->exists($redisKey)) {
                    error_log("ℹ️ [SNAP] Bỏ qua vì gương mặt này đã được xử lý gần đây.");
                    $database->insert('mqtt_messages', $snapData);
                    echo "✅ [SNAP] Đã ghi sự kiện Snap bị khóa vào logs.\n";
                    return;
                }

                $newSn = uniqid('NV_');
                $imagePaths = save_image_from_base64($picBase64, $facesUploadPath, $photosUploadPath, 'snap_', $newSn);
                
                if ($imagePaths === null) {
                    error_log("⚠️ [Snap] Không thể lưu ảnh. Dừng quá trình tự động đăng ký.");
                    $database->insert('mqtt_messages', $snapData);
                    echo "✅ [SNAP] Đã ghi sự kiện Snap không lưu được ảnh vào logs.\n";
                    return;
                }

                // Cập nhật snapData với trạng thái thành công
                $snapData['person_name'] = 'Người lạ (Auto-Reg)';
                $snapData['image_path'] = $imagePaths['faces_path'];
                $database->insert('mqtt_messages', $snapData);
                echo "✅ [SNAP] Đã ghi nhận sự kiện Snap vào logs.\n";

                $newPersonName = 'Người lạ ' . date('d/m H:i');
                try {
                    $database->insert("employee", [
                        'sn' => $newSn,
                        'person_name' => $newPersonName,
                        'registration_photo' => $imagePaths['photos_path'],
                    ]);
                    echo "✅ [SNAP] Đã tự động thêm nhân viên mới. SN: {$newSn}\n";

                    $publicImageUrl = $publicBaseUrl . '/' . $imagePaths['photos_path'];
                    $mqttPayload = [
                        "messageId" => uniqid(),
                        "operator" => "EditPerson",
                        "info" => [ "customId" => $newSn, "name" => $newPersonName, "personType" => 0, "picURI" => $publicImageUrl ]
                    ];
                    publishMqttMessage($env, 'mqtt/face/1018656', $mqttPayload);

                    // Đặt khóa Redis sau khi xử lý thành công
                    $redis->setex($redisKey, 300, 1);

                } catch (Exception $e) {
                    if (file_exists($facesUploadPath . basename($imagePaths['faces_path']))) {
                        unlink($facesUploadPath . basename($imagePaths['faces_path']));
                    }
                    if (file_exists($photosUploadPath . basename($imagePaths['photos_path']))) {
                        unlink($photosUploadPath . basename($imagePaths['photos_path']));
                    }
                    error_log("❌ [SNAP] Lỗi DB khi tự động thêm nhân viên: " . $e->getMessage());
                }
            } finally {
                // Giải phóng khóa
                $redis->del($lockKey);
            }
        }
    }, 0);

    echo "✅ [LISTENER] Kết nối MQTT thành công. Đang lắng nghe trên topic: {$wildcardTopic}\n";
    $mqtt->loop(true);

} catch (Exception $e) {
    die("FATAL ERROR: Không thể kết nối đến MQTT Broker: " . $e->getMessage());
}