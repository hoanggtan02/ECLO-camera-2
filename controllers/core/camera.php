<?php
if (!defined('ECLO')) die("Hacking attempt");

$jatbi = new Jatbi($app);
$setting = $app->getValueData('setting');


$app->router("/camera/rec", 'GET', function($vars) use ($app, $jatbi, $setting) {
    $vars['title'] = $jatbi->lang('Dữ liệu nhận diện');
    echo $app->render('templates/camera/rec.html', $vars);  
})->setPermissions(['rec']);

$app->router("/camera/rec", 'POST', function($vars) use ($app, $jatbi) {
    $app->header(['Content-Type' => 'application/json']);

    $draw = $_POST['draw'] ?? 0;
    $start = $_POST['start'] ?? 0;
    $length = $_POST['length'] ?? 10;
    $searchValue = $_POST['search']['value'] ?? '';
    $dateFrom = $_POST['date_from'] ?? '';
    $dateTo = $_POST['date_to'] ?? '';
    $personType = $_POST['person_type'] ?? '';
    $orderColumnIndex = $_POST['order'][0]['column'] ?? 0;
    $orderDir = strtoupper($_POST['order'][0]['dir'] ?? 'DESC');
    $validColumns = ["id", "image_path", "person_name", "person_id", "similarity", "event_time", "person_type", "is_no_mask"];
    $orderColumn = $validColumns[$orderColumnIndex] ?? "id";

    $conditions = [
        'event_type' => 'Rec' 
    ]; 

    if (!empty($searchValue)) {
        $conditions["OR"] = [
            "person_name[~]" => $searchValue,
            "person_id[~]" => $searchValue
        ];
    }

    if (!empty($dateFrom) && !empty($dateTo)) {
        $conditions["event_time[<>]"] = [$dateFrom . " 00:00:00", $dateTo . " 23:59:59"];
    }

    if ($personType !== '') {
        $conditions["person_type"] = $personType;
    }

    $where = [
        "LIMIT" => [$start, $length],
        "ORDER" => [$orderColumn => $orderDir]
    ];
    
    if (!empty($conditions)) {
        $where["AND"] = $conditions;
    }

    $totalRecords = $app->count("mqtt_messages", ['event_type' => 'Rec']);
    $filteredRecords = $app->count("mqtt_messages", !empty($conditions) ? ["AND" => $conditions] : []);
    $datas = $app->select("mqtt_messages", "*", $where) ?? [];
    
    $formattedData = array_map(function($data) use ($app, $jatbi) {
        $personTypes = [
            "0" => $jatbi->lang("Nhân viên"), 
            "1" => $jatbi->lang("Người lạ"),
        ];
        $imageHtml = $data['image_path']
            ? '<img src="/public/'.$data['image_path'].'" alt="Face" class="img-thumbnail" style="width: 60px; height: auto;">'
            : $jatbi->lang('Không có ảnh');

        return [
            "id" => $data['id'],
            "image_path" => $imageHtml,
            "person_name" => $data['person_name'],
            "person_id" => $data['person_id'],
            "similarity" => number_format($data['similarity'], 2) . '%',
            "event_time" => date('H:i:s d/m/Y', strtotime($data['event_time'])),
            "person_type" => $personTypes[$data['person_type']] ?? $jatbi->lang("Không xác định"),
            "is_no_mask" => $data['is_no_mask'] ? '<span class="badge bg-danger">' . $jatbi->lang("Có khẩu trang") . '</span>' : '<span class="badge bg-success">' . $jatbi->lang("Không khẩu trang") . '</span>',        
        ];
    }, $datas);

    echo json_encode([
        "draw" => intval($draw),
        "recordsTotal" => $totalRecords,
        "recordsFiltered" => $filteredRecords,
        "data" => $formattedData
    ]);
})->setPermissions(['rec']);


$app->router("/camera/snap", 'GET', function($vars) use ($app, $jatbi, $setting) {
    $vars['title'] = $jatbi->lang('Dữ liệu ảnh chụp nhanh');
    echo $app->render('templates/camera/snap.html', $vars);
})->setPermissions(['snap']);
    
$app->router("/camera/snap", 'POST', function($vars) use ($app, $jatbi) {
    $app->header(['Content-Type' => 'application/json']);

    $draw = $_POST['draw'] ?? 0;
    $start = $_POST['start'] ?? 0;
    $length = $_POST['length'] ?? 10;
    $searchValue = $_POST['search']['value'] ?? '';
    $dateFrom = $_POST['date_from'] ?? '';
    $dateTo = $_POST['date_to'] ?? '';
    $orderColumnIndex = $_POST['order'][0]['column'] ?? 0;
    $orderDir = strtoupper($_POST['order'][0]['dir'] ?? 'DESC');
    $validColumns = ["id", "image_path", "record_id", "event_time", "is_no_mask"];
    $orderColumn = $validColumns[$orderColumnIndex] ?? "id";
        $conditions = [
        'event_type' => 'Snap'
    ]; 

    if (!empty($dateFrom) && !empty($dateTo)) {
        $conditions["event_time[<>]"] = [$dateFrom . " 00:00:00", $dateTo . " 23:59:59"];
    }

    $where = [
        "LIMIT" => [$start, $length],
        "ORDER" => [$orderColumn => $orderDir]
    ];
    
    if (!empty($conditions)) {
        $where["AND"] = $conditions;
    }

    $totalRecords = $app->count("mqtt_messages", ['event_type' => 'Snap']);
    $filteredRecords = $app->count("mqtt_messages", !empty($conditions) ? ["AND" => $conditions] : []);
    $datas = $app->select("mqtt_messages", "*", $where) ?? [];
    
    // Định dạng dữ liệu chỉ cho Snap
    $formattedData = array_map(function($data) use ($app, $jatbi) {
        $imageHtml = $data['image_path']
            ? '<img src="/public/' . $data['image_path'] . '" alt="Face" class="img-thumbnail" style="width: 60px; height: auto;">'
            : $jatbi->lang('Không có ảnh');

        return [
            "id" => $data['id'],
            "image_path" => $imageHtml,
            "person_name" => 'Người lạ',
            "record_id" => $data['record_id'],
            "event_time" => date('H:i:s d/m/Y', strtotime($data['event_time'])),
            "is_no_mask" => $data['is_no_mask'] ? '<span class="badge bg-danger">' . $jatbi->lang("Có khẩu trang") . '</span>' : '<span class="badge bg-success">' . $jatbi->lang("Không khẩu trang") . '</span>',        
        ];
    }, $datas);

    echo json_encode([
        "draw" => intval($draw),
        "recordsTotal" => $totalRecords,
        "recordsFiltered" => $filteredRecords,
        "data" => $formattedData
    ]);
})->setPermissions(['snap']);

