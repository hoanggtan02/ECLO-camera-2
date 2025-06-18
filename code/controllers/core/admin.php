<?php
    if (!defined('ECLO')) die("Hacking attempt");
    $jatbi = new Jatbi($app);
    $setting = $app->getValueData('setting');
    $common = $app->getValueData('common');

    $app->router("/admin/blockip", 'GET', function($vars) use ($app, $jatbi) {
        $vars['title'] = $jatbi->lang("Chặn truy cập");
        $vars['datatable'] = $app->component('datatable',["datas"=>[],"search"=>[]]);
        echo $app->render('templates/admin/blockip.html', $vars);
    })->setPermissions(['blockip']);

    $app->router("/admin/blockip", 'POST', function($vars) use ($app, $jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $draw = isset($_POST['draw']) ? intval($_POST['draw']) : 0;
        $start = isset($_POST['start']) ? intval($_POST['start']) : 0;
        $length = isset($_POST['length']) ? intval($_POST['length']) : 10;
        $searchValue = isset($_POST['search']['value']) ? $_POST['search']['value'] : '';
        $orderName = isset($_POST['order'][0]['name']) ? $_POST['order'][0]['name'] : 'id';
        $orderDir = isset($_POST['order'][0]['dir']) ? $_POST['order'][0]['dir'] : 'DESC';
        $status = isset($_POST['status']) ? [$_POST['status'],$_POST['status']] : '';
        $where = [
            "AND" => [
                "OR" => [
                    "ip[~]" => $searchValue,
                    "notes[~]" => $searchValue,
                ],
                "status[<>]" => $status,
                "deleted" => 0,
            ],
            "LIMIT" => [$start, $length],
            "ORDER" => [$orderName => strtoupper($orderDir)]
        ];
        $count = $app->count("blockip",["AND" => $where['AND']]);
        $app->select("blockip", "*", $where, function ($data) use (&$datas, $jatbi,$app) {
            $datas[] = [
                "checkbox" => $app->component("box",["data"=>$data['active']]),
                "ip" => $data['ip'],
                "date" => $data['date'],
                "status" => $app->component("status",["url"=>"/admin/blockip-status/".$data['active'],"data"=>$data['status'],"permission"=>['blockip.edit']]),
                "action" => $app->component("action",[
                            "button" => [
                                [
                                    'type' => 'button',
                                    'name' => $jatbi->lang("Sửa"),
                                    'permission' => ['blockip.edit'],
                                    'action' => ['data-url' => '/admin/blockip-edit/'.$data['active'], 'data-action' => 'modal']
                                ],
                                [
                                    'type' => 'button',
                                    'name' => $jatbi->lang("Xóa"),
                                    'permission' => ['blockip.deleted'],
                                    'action' => ['data-url' => '/admin/blockip-deleted?box='.$data['active'], 'data-action' => 'modal']
                                ],
                            ]
                        ]),
            ];
        });
        echo json_encode([
            "draw" => $draw,
            "recordsTotal" => $count,
            "recordsFiltered" => $count,
            "data" => $datas ?? []
        ]);
    })->setPermissions(['blockip']);

    $app->router("/admin/blockip-add", 'GET', function($vars) use ($app, $jatbi) {
        $vars['title'] = $jatbi->lang("Thêm Chặn truy cập");
        $vars['data'] = [
            "status" => 'A',
        ];
        echo $app->render('templates/admin/blockip-post.html', $vars, 'global');
    })->setPermissions(['blockip.add']);

    $app->router("/admin/blockip-add", 'POST', function($vars) use ($app, $jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        if($app->xss($_POST['ip'])=='' || $app->xss($_POST['status'])==''){
            $error = ["status"=>"error","content"=>$jatbi->lang("Vui lòng không để trống")];
        }
        if(empty($error)){
            $insert = [
                "ip"          => $app->xss($_POST['ip']),
                "status"        => $app->xss($_POST['status']),
                "notes"         => $app->xss($_POST['notes']),
                "date"          => date("Y-m-d H:i:s"),
                "active"        => $jatbi->active(),
            ];
            $app->insert("blockip",$insert);
            echo json_encode(['status'=>'success','content'=>$jatbi->lang("Cập nhật thành công")]);
            $jatbi->logs('blockip','blockip-add',$insert);
        }
        else {
            echo json_encode($error);
        }
    })->setPermissions(['blockip.add']);

    $app->router("/admin/blockip-edit/{id}", 'GET', function($vars) use ($app, $jatbi) {
        $vars['title'] = $jatbi->lang("Sửa Chặn truy cập");
        $vars['data'] = $app->get("blockip","*",["active"=>$vars['id'],"deleted"=>0]);
        if($vars['data']>1){
            echo $app->render('templates/admin/blockip-post.html', $vars, 'global');
        }
        else {
            echo $app->render('templates/common/error-modal.html', $vars, 'global');
        }
    })->setPermissions(['blockip.edit']);

    $app->router("/admin/blockip-edit/{id}", 'POST', function($vars) use ($app, $jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $data = $app->get("blockip","*",["active"=>$vars['id'],"deleted"=>0]);
        if($data>1){
            if($app->xss($_POST['ip'])=='' || $app->xss($_POST['status'])==''){
                $error = ["status"=>"error","content"=>$jatbi->lang("Vui lòng không để trống")];
            }
            if(empty($error)){
                $insert = [
                    "ip"          => $app->xss($_POST['ip']),
                    "status"        => $app->xss($_POST['status']),
                    "notes"         => $app->xss($_POST['notes']),
                    "date"          => date("Y-m-d H:i:s"),
                ];
                $app->update("blockip",$insert,["id"=>$data['id']]);
                echo json_encode(['status'=>'success','content'=>$jatbi->lang("Cập nhật thành công")]);
                $jatbi->logs('blockip','blockip-edit',$insert);
            }
            else {
                echo json_encode($error);
            }
        }
        else {
            echo json_encode(["status"=>"error","content"=>$jatbi->lang("Không tìm thấy dữ liệu")]);
        }
    })->setPermissions(['blockip.edit']);

    $app->router("/admin/blockip-status/{id}", 'POST', function($vars) use ($app, $jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $data = $app->get("blockip","*",["active"=>$vars['id'],"deleted"=>0]);
        if($data>1){
            if($data>1){
                if($data['status']==='A'){
                    $status = "D";
                } 
                elseif($data['status']==='D'){
                    $status = "A";
                }
                $app->update("blockip",["status"=>$status],["id"=>$data['id']]);
                $jatbi->logs('blockip','blockip-status',$data);
                echo json_encode(['status'=>'success','content'=>$jatbi->lang("Cập nhật thành công")]);
            }
            else {
                echo json_encode(['status'=>'error','content'=>$jatbi->lang("Cập nhật thất bại"),]);
            }
        }
        else {
            echo json_encode(["status"=>"error","content"=>$jatbi->lang("Không tìm thấy dữ liệu")]);
        }
    })->setPermissions(['blockip.edit']);

    $app->router("/admin/blockip-deleted", 'GET', function($vars) use ($app, $jatbi) {
        $vars['title'] = $jatbi->lang("Xóa Tài khoản");
        echo $app->render('templates/common/deleted.html', $vars, 'global');
    })->setPermissions(['blockip.deleted']);

    $app->router("/admin/blockip-deleted", 'POST', function($vars) use ($app,$jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $boxid = explode(',', $app->xss($_GET['box']));
        $datas = $app->select("blockip","*",["active"=>$boxid,"deleted"=>0]);
        if(count($datas)>0){
            foreach($datas as $data){
                $app->update("blockip",["deleted"=> 1],["id"=>$data['id']]);
                $name[] = $data['ip'];
            }
            $jatbi->logs('blockip','blockip-deleted',$datas);
            $jatbi->trash('/admin/blockip-restore',"Chặn truy cập với ip: ".implode(', ',$name),["database"=>'blockip',"data"=>$boxid]);
            echo json_encode(['status'=>'success',"content"=>$jatbi->lang("Cập nhật thành công")]);
        }
        else {
            echo json_encode(['status'=>'error','content'=>$jatbi->lang("Có lỗi xẩy ra")]);
        }
    })->setPermissions(['blockip.deleted']);

    $app->router("/admin/blockip-restore/{id}", 'GET', function($vars) use ($app, $jatbi) {
        $vars['data'] = $app->get("trashs","*",["active"=>$vars['id'],"deleted"=>0]);
        if($vars['data']>1){
            echo $app->render('templates/common/restore.html', $vars, 'global');
        }
        else {
            echo $app->render('templates/common/error-modal.html', $vars, 'global');
        }
    })->setPermissions(['blockip.deleted']);

    $app->router("/admin/blockip-restore/{id}", 'POST', function($vars) use ($app,$jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $trash = $app->get("trashs","*",["active"=>$vars['id'],"deleted"=>0]);
        if($trash>1){
            $datas = json_decode($trash['data']);
            foreach($datas->data as $active) {
                $app->update("blockip",["deleted"=>0],["active"=>$active]);
            }
            $app->delete("trashs",["id"=>$trash['id']]);
            $jatbi->logs('blockip','blockip-restore',$datas);
            echo json_encode(['status'=>'success',"content"=>$jatbi->lang("Cập nhật thành công")]);
        }
        else {
            echo json_encode(['status'=>'error','content'=>$jatbi->lang("Có lỗi xẩy ra")]);
        }
    })->setPermissions(['blockip.deleted']);

    $app->router("/admin/logs", 'GET', function($vars) use ($app, $jatbi) {
        $vars['title'] = $jatbi->lang("Nhật ký");
        echo $app->render('templates/admin/logs.html', $vars);
    })->setPermissions(['logs']);

    $app->router("/admin/logs", 'POST', function($vars) use ($app, $jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $draw = isset($_POST['draw']) ? intval($_POST['draw']) : 0;
        $start = isset($_POST['start']) ? intval($_POST['start']) : 0;
        $length = isset($_POST['length']) ? intval($_POST['length']) : 10;
        $searchValue = isset($_POST['search']['value']) ? $_POST['search']['value'] : '';
        $orderName = isset($_POST['order'][0]['name']) ? $_POST['order'][0]['name'] : 'id';
        $orderDir = isset($_POST['order'][0]['dir']) ? $_POST['order'][0]['dir'] : 'desc';
        $dateRange = isset($_GET['date']) ? $_GET['date'] : null;
        $date_from = null;
        $date_to = null;
        if ($dateRange) {
            if (is_array($dateRange) && count($dateRange) == 2) {
                $date_from = date('Y-m-d 00:00:00', strtotime($dateRange[0]));
                $date_to = date('Y-m-d 23:59:59', strtotime($dateRange[1]));
            } elseif (is_string($dateRange)) {
                $date_from = date('Y-m-d 00:00:00', strtotime($dateRange));
                $date_to = date('Y-m-d 23:59:59', strtotime($dateRange));
            }
        }
        $where = [
            "AND" => [
                "OR" => [
                    "logs.dispatch[~]" => $searchValue,
                    "logs.action[~]" => $searchValue,
                    "logs.content[~]" => $searchValue,
                    "logs.url[~]" => $searchValue,
                    "logs.ip[~]" => $searchValue,
                    "accounts.name[~]" => $searchValue,
                ],
            ],
            "LIMIT" => [$start, $length],
            "ORDER" => [$orderName => strtoupper($orderDir)]
        ];
        if ($date_from && $date_to) {
            $where['AND']["logs.date[<>]"] = [$date_from, $date_to];
        }
        $count = $app->count("logs", [
            "[>]accounts" => ["user" => "id"]
        ], [
            "logs.id"
        ], $where['AND']);
        $app->select("logs", [
                "[>]accounts" => ["user" => "id"]
            ], 
            [
            'logs.id',
            'logs.dispatch',
            'logs.action',
            'logs.url',
            'logs.ip',
            'logs.date',
            'logs.user',
            'logs.active',
            'accounts.name',
            'accounts.avatar',
            ], $where, function ($data) use (&$datas,$jatbi) {
                $datas[] = [
                    "user" => '<img src="/' . $data['avatar'] . '" class="width rounded-circle me-2" style="--width:40px"> '.$data['name'],
                    "dispatch" => $data['dispatch'],
                    "action" => $data['action'],
                    "url" => $data['url'],
                    "ip" => $data['ip'],
                    "date" => $data['date'],
                    "views" => '<button data-action="modal" data-url="/admin/logs-views/'.$data['active'].'" class="btn btn-primary-light btn-sm border-0 py-1 px-2 rounded-3" aria-label="'.$jatbi->lang('Xem').'"><i class="ti ti-eye"></i></button>',
                ];
        });
        echo json_encode([
            "draw" => $draw,
            "recordsTotal" => $count,
            "recordsFiltered" => $count,
            "data" => $datas ?? []
        ]);
    })->setPermissions(['logs']);

    $app->router("/admin/logs-views/{id}", 'GET', function($vars) use ($app, $jatbi) {
        $vars['data'] = $app->get("logs","*",["active"=>$vars['id'],"deleted"=>0]);
        if($vars['data']>1){
            echo $app->render('templates/admin/logs-views.html', $vars, 'global');
        }
        else {
            echo $app->render('templates/common/error-modal.html', $vars, 'global');
        }
    })->setPermissions(['blockip.edit']);

    $app->router("/admin/trash", 'GET', function($vars) use ($app, $jatbi) {
        $vars['title'] = $jatbi->lang("Thùng rác");
        echo $app->render('templates/admin/trash.html', $vars);
    })->setPermissions(['trash']);

    $app->router("/admin/trash", 'POST', function($vars) use ($app, $jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $draw = isset($_POST['draw']) ? intval($_POST['draw']) : 0;
        $start = isset($_POST['start']) ? intval($_POST['start']) : 0;
        $length = isset($_POST['length']) ? intval($_POST['length']) : 10;
        $searchValue = isset($_POST['search']['value']) ? $_POST['search']['value'] : '';
        $orderName = isset($_POST['order'][0]['name']) ? $_POST['order'][0]['name'] : 'id';
        $orderDir = isset($_POST['order'][0]['dir']) ? $_POST['order'][0]['dir'] : 'desc';
        $dateRange = isset($_GET['date']) ? $_GET['date'] : null;
        $date_from = null;
        $date_to = null;
        if ($dateRange) {
            if (is_array($dateRange) && count($dateRange) == 2) {
                $date_from = date('Y-m-d 00:00:00', strtotime($dateRange[0]));
                $date_to = date('Y-m-d 23:59:59', strtotime($dateRange[1]));
            } elseif (is_string($dateRange)) {
                $date_from = date('Y-m-d 00:00:00', strtotime($dateRange));
                $date_to = date('Y-m-d 23:59:59', strtotime($dateRange));
            }
        }
        $where = [
            "AND" => [
                "OR" => [
                    "trashs.content[~]" => $searchValue,
                    "accounts.name[~]" => $searchValue,
                ],
            ],
            "LIMIT" => [$start, $length],
            "ORDER" => [$orderName => strtoupper($orderDir)]
        ];
        if ($date_from && $date_to) {
            $where['AND']["trashs.date[<>]"] = [$date_from, $date_to];
        }
        $count = $app->count("trashs", [
            "[>]accounts" => ["account" => "id"]
        ], [
            "trashs.id"
        ], $where['AND']);
        $app->select("trashs", [
                "[>]accounts" => ["account" => "id"]
            ], 
            [
            'trashs.id',
            'trashs.content',
            'trashs.url',
            'trashs.ip',
            'trashs.date',
            'trashs.active',
            'trashs.router',
            'accounts.name',
            'accounts.avatar',
            ], $where, function ($data) use (&$datas,$jatbi,$app) {
                $datas[] = [
                    "checkbox" => $app->component("box",["data"=>$data['active']]),
                    "user" => '<img src="/' . $data['avatar'] . '" class="width rounded-circle me-2" style="--width:40px"> '.$data['name'],
                    "content" => $data['content'],
                    "ip" => $data['ip'],
                    "date" => $data['date'],
                    "action" => $app->component("action",[
                        "button" => [
                            [
                                'type' => 'button',
                                'name' => $jatbi->lang("Phục hồi"),
                                'permission' => ['trash'],
                                'action' => ['data-url' => $data['router'].'/'.$data['active'], 'data-action' => 'modal']
                            ],
                            [
                                'type' => 'button',
                                'name' => $jatbi->lang("Xóa vĩnh viễn"),
                                'permission' => ['trash'],
                                'action' => ['data-url' => '/admin/trash-deleted?box='.$data['active'], 'data-action' => 'modal']
                            ],
                        ]
                    ]),
                ];
        });
        echo json_encode([
            "draw" => $draw,
            "recordsTotal" => $count,
            "recordsFiltered" => $count,
            "data" => $datas ?? []
        ]);
    })->setPermissions(['trash']);

    $app->router("/admin/trash-deleted", 'GET', function($vars) use ($app, $jatbi) {
        $vars['title'] = $jatbi->lang("Xóa thùng rác");
        echo $app->render('templates/common/deleted.html', $vars, 'global');
    })->setPermissions(['trash']);

    $app->router("/admin/trash-deleted", 'POST', function($vars) use ($app,$jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $boxid = explode(',', $app->xss($_GET['box']));
        $datas = $app->select("trashs","*",["active"=>$boxid,"deleted"=>0]);
        if(count($datas)>0){
            foreach($datas as $data){
                $app->delete("trashs",["id"=>$data['id']]);
            }
            $jatbi->logs('trash','deleted',$datas);
            echo json_encode(['status'=>'success',"content"=>$jatbi->lang("Cập nhật thành công")]);
        }
        else {
            echo json_encode(['status'=>'error','content'=>$jatbi->lang("Có lỗi xẩy ra")]);
        }
    })->setPermissions(['trash']);
?>