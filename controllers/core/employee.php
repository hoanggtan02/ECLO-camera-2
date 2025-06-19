<?php

use PhpMqtt\Client\MqttClient;
use PhpMqtt\Client\ConnectionSettings;

if (!defined('ECLO')) die("Hacking attempt");

$jatbi = new Jatbi($app);
$setting = $app->getValueData('setting');

// --- HÀM HELPER ĐỂ GỬI TIN NHẮN MQTT ---
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
    $mqttClientId = 'eclo-web-publisher-' . uniqid();

    try {
        $mqtt = new MqttClient($mqttServer, $mqttPort, $mqttClientId);
        $connectionSettings = (new ConnectionSettings)
            ->setUsername($mqttUsername)
            ->setPassword($mqttPassword)
            ->setConnectTimeout(5);
        $mqtt->connect($connectionSettings, true);
        $mqtt->publish($topic, json_encode($payload, JSON_UNESCAPED_UNICODE), 0);
        $mqtt->disconnect();
        error_log("✅ MQTT Publish Success to topic [{$topic}]: " . json_encode($payload));
        return true;
    } catch (Exception $e) {
        error_log("❌ MQTT Publish Error: " . $e->getMessage());
        return false;
    }
}


// ROUTE GET /employee (Hiển thị trang chính)
$app->router("/employee", 'GET', function($vars) use ($app, $jatbi, $setting) {
    $vars['title'] = $jatbi->lang('Quản lý nhân viên');
    echo $app->render('templates/camera/employee.html', $vars);
})->setPermissions(['employee']);


// ROUTE POST /employee (Xử lý DataTables)
$app->router("/employee", 'POST', function($vars) use ($app, $jatbi) {
    $app->header(['Content-Type' => 'application/json']);

    $draw = $_POST['draw'] ?? 0;
    $start = $_POST['start'] ?? 0;
    $length = $_POST['length'] ?? 10;
    $searchValue = $_POST['search']['value'] ?? '';
    $orderColumnIndex = $_POST['order'][0]['column'] ?? 1;
    $orderDir = strtoupper($_POST['order'][0]['dir'] ?? 'DESC');
    $validColumns = [
        "creation_time", // 0: Mặc định cho checkbox
        "sn",            // 1
        "registration_photo", // 2
        "person_name",   // 3
        "telephone",     // 4
        "gender",        // 5
        "birthday",      // 6
        "creation_time", // 7
        "creation_time"  // 8: Mặc định cho action
    ];
    $orderColumn = $validColumns[$orderColumnIndex] ?? "creation_time";

    $conditions = [];
    if (!empty($searchValue)) {
        $conditions["OR"] = ["sn[~]" => $searchValue, "person_name[~]" => $searchValue, "telephone[~]" => $searchValue];
    }
    
    // Xây dựng tham số cho Medoo
    $medooSelectOptions = [
        "ORDER" => [$orderColumn => $orderDir],
        "LIMIT" => [$start, $length]
    ];
    if (!empty($conditions)) { 
        $medooSelectOptions["AND"] = $conditions; 
    }

    $totalRecords = $app->count("employee");
    $filteredRecords = $app->count("employee", !empty($conditions) ? ["AND" => $conditions] : []); // Điều kiện tìm kiếm cho filteredRecords

    $datas = $app->select("employee", "*", $medooSelectOptions) ?? []; // Truyền các options đã xây dựng vào select
    
    $envPath = __DIR__ . '/../../.env';
    $env = file_exists($envPath) ? parse_ini_file($envPath) : [];
    $publicBaseUrl = rtrim($env['APP_URL'] ?? '', '/');

    $formattedData = array_map(function($data) use ($app, $jatbi, $publicBaseUrl) {
        $genderLabels = ["1" => $jatbi->lang("Nam"), "2" => $jatbi->lang("Nữ"), "3" => $jatbi->lang("Khác")];
        $imageSrc = (string) ($data['registration_photo'] ?? ''); // Đảm bảo chuỗi, không null
        if (!empty($imageSrc) && strpos($imageSrc, 'http') !== 0) {
            $imageSrc = $publicBaseUrl . '/' . ltrim($imageSrc, '/');
        }
        $photoHtml = !empty($imageSrc) ? '<img src="public/' . htmlspecialchars($imageSrc) . '" alt="Photo" class="img-thumbnail" style="width: 60px; height: auto;">' : $jatbi->lang('Chưa có ảnh');
        
        // --- BẮT ĐẦU SỬA LỖI DEPRECATED ---
        $sn_val = (string) ($data['sn'] ?? '');
        $person_name_val = (string) ($data['person_name'] ?? '');
        $telephone_val = (string) ($data['telephone'] ?? '');
        $gender_val = (string) ($data['gender'] ?? '');
        $birthday_val = (string) ($data['birthday'] ?? '');
        $creation_time_val = (string) ($data['creation_time'] ?? '');



        return [
            "checkbox" => $app->component("box", ["data" => $data['id']]),
            "sn" => htmlspecialchars($sn_val),
            "registration_photo" => $photoHtml,
            "person_name" => htmlspecialchars($person_name_val),
            "telephone" => htmlspecialchars($telephone_val),
            "gender" => $genderLabels[$gender_val] ?? $jatbi->lang("Không xác định"),
            "birthday" => $birthday_val ? date('d/m/Y', strtotime($birthday_val)) : 'N/A',
            "creation_time" => $creation_time_val ? date('H:i:s d/m/Y', strtotime($creation_time_val)) : 'N/A',
            "action" => $app->component("action", [
                "button" => [
                    [
                        'type' => 'button',
                        'name' => $jatbi->lang("Sửa"),
                        'permission' => ['employee'],
                        'action' => [
                            'data-url' => '/admin/library-edit?id=' . $data['id'],
                            'data-action' => 'modal'
                        ]
                    ],
                    [
                        'type' => 'button',
                        'name' => $jatbi->lang("Xóa"),
                        'permission' => ['employee'],
                        'action' => [
                            'data-url' => '/admin/library-delete?id=' . $data['id'],
                            'data-action' => 'modal'
                        ]
                    ]
                ]
            ])
        ];
    }, $datas);

    echo json_encode(["draw" => intval($draw), "recordsTotal" => $totalRecords, "recordsFiltered" => $filteredRecords, "data" => $formattedData]);
})->setPermissions(['employee']);


// ROUTE GET /employee-add (Hiển thị form thêm)
$app->router("/employee-add", 'GET', function($vars) use ($app, $jatbi) {
    $vars['title'] = $jatbi->lang('Thêm nhân viên');
    
    $htmlContent = $app->render('templates/camera/employee-post.html', $vars, 'global');
    
    echo json_encode(['content' => $htmlContent]);

})->setPermissions(['employee']);


// ROUTE POST /employee-add (Xử lý thêm nhân viên và publish MQTT)
$app->router("/employee-add", 'POST', function($vars) use ($app, $jatbi) {
    $app->header(['Content-Type' => 'application/json']);

    $envPath = __DIR__ . '/../../.env';
    if (!file_exists($envPath)) {
        echo json_encode(['status' => 'error', 'content' => $jatbi->lang('Lỗi cấu hình server.')]);
        return;
    }
    $env = parse_ini_file($envPath);
    $publicBaseUrl = rtrim($env['APP_URL'] ?? '', '/');

    // --- Lấy dữ liệu và Validation ---
    $person_name = trim($_POST['person_name'] ?? '');
    if (empty($person_name)) {
        echo json_encode(['status' => 'error', 'content' => $jatbi->lang('Vui lòng nhập họ và tên nhân viên.')]);
        return;
    }

    if (!isset($_FILES['registration_photo']) || $_FILES['registration_photo']['error'] !== UPLOAD_ERR_OK) {
        echo json_encode(['status' => 'error', 'content' => $jatbi->lang('Vui lòng chọn ảnh đăng ký.')]);
        return;
    }

    $max_size_kb = 300;
    if ($_FILES['registration_photo']['size'] > ($max_size_kb * 1024)) {
        echo json_encode(['status' => 'error', 'content' => $jatbi->lang('Kích thước ảnh không được vượt quá ' . $max_size_kb . 'KB.')]);
        return;
    }
    
    if (!function_exists('mime_content_type')) {
        error_log("Lỗi: Extension 'fileinfo' của PHP không được kích hoạt.");
        echo json_encode(['status' => 'error', 'content' => $jatbi->lang('Lỗi server: Thiếu extension fileinfo.')]);
        return;
    }
    $allowed_types = ['image/jpeg', 'image/png'];
    $file_type = mime_content_type($_FILES['registration_photo']['tmp_name']);
    if (!in_array($file_type, $allowed_types)) {
        echo json_encode(['status' => 'error', 'content' => $jatbi->lang('Định dạng ảnh không hợp lệ. Chỉ chấp nhận JPG, PNG.')]);
        return;
    }
    
    $sn = $_POST['sn'] ?? 'NV' . time();
    if ($app->has("employee", ["sn" => $sn])) {
        echo json_encode(['status' => 'error', 'content' => $jatbi->lang('Mã nhân viên này đã tồn tại.')]);
        return;
    }

    // --- Xử lý tải file ảnh ---
    $uploadDir = __DIR__ . '/../../public/uploads/photos/';
    if (!is_dir($uploadDir)) mkdir($uploadDir, 0777, true);
    
    $fileExtension = pathinfo($_FILES['registration_photo']['name'], PATHINFO_EXTENSION);
    $newFileName = $sn . '_' . uniqid() . '.' . strtolower($fileExtension);
    $uploadFilePath = $uploadDir . $newFileName;
    $dbImagePath = 'uploads/photos/' . $newFileName;
    $publicImageUrl = $publicBaseUrl . '/' . $dbImagePath;

    if (!move_uploaded_file($_FILES['registration_photo']['tmp_name'], $uploadFilePath)) {
        echo json_encode(['status' => 'error', 'content' => $jatbi->lang('Lỗi khi tải ảnh lên.')]);
        return;
    }

    try {
        // --- Thêm vào Database ---
        $dataToInsert = [
            "sn" => $sn,
            "person_name" => $person_name,
            "registration_photo" => $dbImagePath,
            "telephone" => $_POST['telephone'] ?? null,
            "gender" => $_POST['gender'] ?? null,
            "birthday" => empty($_POST['birthday']) ? null : $_POST['birthday'],
            "id_card" => $_POST['id_card'] ?? null,
            "address" => $_POST['address'] ?? null,
        ];
        $app->insert("employee", $dataToInsert);

        // --- Gửi tin nhắn MQTT ---
        $mqttGender = null;
        if (($dataToInsert['gender'] ?? '') === '1') $mqttGender = 0;
        elseif (($dataToInsert['gender'] ?? '') === '2') $mqttGender = 1;

        $mqttPayload = [
            "messageId" => uniqid(),
            "operator" => "EditPerson",
            "info" => [
                "customId" => $sn,
                "name" => $person_name,
                "gender" => $mqttGender,
                "birthday" => $dataToInsert['birthday'] ?? "",
                "address" => $dataToInsert['address'] ?? "",
                "idCard" => $dataToInsert['id_card'] ?? "",
                "telnum1" => $dataToInsert['telephone'] ?? "",
                "personType" => 0, 
                "picURI" => "https://demo.sfit.vn/images/customers/z6700531755512_62215e7fdfcc4756e8cc56f37d48e1eb_5.jpg",
            ]
        ];
        publishMqttMessage($env, 'mqtt/face/1018656', $mqttPayload);
        
        echo json_encode(["status" => "success", "content" => $jatbi->lang("Thêm nhân viên thành công"), 'load' => '/employee']);

    } catch (Exception $e) {
        if (file_exists($uploadFilePath)) unlink($uploadFilePath);
        error_log("DB Error on employee add: " . $e->getMessage());
        echo json_encode(["status" => "error", "content" => $jatbi->lang("Lỗi database khi thêm nhân viên.")]);
    }
})->setPermissions(['employee']);


// ROUTE POST /employee-delete (Xóa nhân viên và đồng bộ MQTT)
$app->router("/employee-delete", 'POST', function($vars) use ($app, $jatbi) {
    $app->header(['Content-Type' => 'application/json']);
    $list = explode(',', $_POST['list'] ?? '');
    
    if (empty($list) || empty($list[0])) {
        echo json_encode(['status' => 'error', 'content' => $jatbi->lang('Chưa chọn nhân viên.')]);
        return;
    }
    
    // Đọc file .env để có cấu hình MQTT
    $envPath = __DIR__ . '/../../.env';
    $env = file_exists($envPath) ? parse_ini_file($envPath) : [];

    try {
        $employeesToDelete = $app->select("employee", ["sn", "registration_photo"], ["sn" => $list]);
        if(empty($employeesToDelete)) return; // Không có gì để xóa

        $app->delete("employee", ["sn" => $list]);

        foreach ($employeesToDelete as $emp) {
            // Xóa file ảnh vật lý
            if (!empty($emp['registration_photo'])) {
                $filePath = __DIR__ . '/../../public/' . $emp['registration_photo'];
                if (file_exists($filePath)) unlink($filePath);
            }
            
            // Gửi lệnh xóa đến MQTT
            $mqttPayload = [
                "messageId" => uniqid(),
                "operator" => "DeletePerson",
                "info" => ["customId" => $emp['sn']]
            ];
            publishMqttMessage($env, 'mqtt/face/1018656', $mqttPayload);
        }
        
        echo json_encode(['status' => 'success', 'content' => $jatbi->lang('Đã xóa nhân viên thành công.'), 'load' => 'this']);

    } catch (Exception $e) {
        error_log("Error on employee delete: " . $e->getMessage());
        echo json_encode(['status' => 'error', 'content' => $jatbi->lang('Lỗi khi xóa nhân viên.')]);
    }
})->setPermissions(['employee']);