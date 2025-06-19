<?php
    if (!defined('ECLO')) die("Hacking attempt");
    $jatbi = new Jatbi($app);
    $setting = $app->getValueData('setting');
    $common = $app->getValueData('common');
    $permission = $app->getValueData('permission');

    $app->router("/notification", 'GET', function($vars) use ($app, $jatbi) {
        $vars['templates'] = 'notification';
        $user = $app->getSession("accounts");
        $vars['datas'] = $app->select("notifications","*",["account"=>$user['id'],"deleted"=>0,"ORDER"=>["date"=>"DESC"],"LIMIT"=>20]);
        echo $app->render('templates/users/notification.html', $vars,'global');
    })->setPermissions(['login']);

    $app->router("/users/profile", 'GET', function($vars) use ($app, $jatbi,$setting) {
        $vars['title'] = $jatbi->lang("Thông tin");
        $vars['router'] = 'profile';
        $vars['account'] = $app->get("accounts","*",["id"=>$app->getSession("accounts")['id']]);
        echo $app->render('templates/users/profile.html', $vars);
    })->setPermissions(['login']);

    $app->router("/users/notification", 'GET', function($vars) use ($app, $jatbi,$setting) {
        $vars['router'] = 'notification';
        $vars['title'] = $jatbi->lang("Thông báo");
        echo $app->render('templates/users/profile.html', $vars);
    })->setPermissions(['login']);

    $app->router("/users/notification", 'POST', function($vars) use ($app, $jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $draw = isset($_POST['draw']) ? intval($_POST['draw']) : 0;
        $start = isset($_POST['start']) ? intval($_POST['start']) : 0;
        $length = isset($_POST['length']) ? intval($_POST['length']) : 10;
        $searchValue = isset($_POST['search']['value']) ? $_POST['search']['value'] : '';
        $orderName = isset($_POST['order'][0]['name']) ? $_POST['order'][0]['name'] : 'id';
        $orderDir = isset($_POST['order'][0]['dir']) ? $_POST['order'][0]['dir'] : 'desc';
        $where = [
            "OR" => [
                "notifications.title[~]" => $searchValue,
                "notifications.content[~]" => $searchValue,
                "accounts.name[~]" => $searchValue,
            ],
            "notifications.account" => $app->getSession("accounts")['id'],
            "notifications.deleted" => 0,
            "LIMIT" => [$start, $length],
            "ORDER" => [$orderName => strtoupper($orderDir)]
        ];
        $count = $app->count("notifications",[
            "OR" => [
                "notifications.title[~]" => $searchValue,
                "notifications.content[~]" => $searchValue,
            ],
            "notifications.account" => $app->getSession("accounts")['id'],
            "notifications.deleted" => 0,
        ]);
        $app->select("notifications", [
                "[>]accounts" => ["user" => "id"]
            ], 
            [
            'notifications.id',
            'notifications.template',
            'notifications.date',
            'notifications.title',
            'notifications.active',
            'notifications.views',
            'notifications.content',
            'notifications.user',
            'accounts.name',
            'accounts.avatar',
            ], $where, function ($data) use (&$datas,$jatbi,$app) {
                if (isset($data['data']) && $data['data'] != '') {
                    $getdata = json_decode($data['data']);
                } else {
                    $getdata = null;
                }
                $content = $jatbi->lang($data['content']);
                $content = str_replace("[account]", $data['name'], $content);
                if ($getdata && isset($getdata->content)) {
                    $content = str_replace("[content]", number_format($getdata->content), $content);
                } else {
                    $content = str_replace("[content]", "0", $content);
                }
                if($data['template']=='url'){
                    $url = '<a class="btn btn-sm btn-primary-light border-0 p-2" href="/users/notification/'.$data['active'].'" data-pjax><i class="ti ti-eye"></i></a>';
                    $content = '<a class="link-primary" href="/users/notification/'.$data['active'].'" data-pjax><span class="width height bg-'.($data['views']>0?'secondary':'danger').' rounded-circle d-inline-block me-2" style="--width:10px;--height:10px"></span>'.$content.'</a>';
                }
                else {
                    $url = '<a class="btn btn-sm btn-primary-light border-0 p-2" data-action="modal" data-url="/users/notification/'.$data['active'].'"><i class="ti ti-eye"></i></a>';
                    $content = '<a class="link-primary" href="/users/notification/'.$data['active'].'" data-pjax><span class="width height bg-'.($data['views']>0?'secondary':'danger').' rounded-circle d-inline-block me-2" style="--width:10px;--height:10px"></span>'.$content.'</a>';
                }
                $datas[] = [
                    "checkbox" => $app->component("box",["data"=>$data['id']]),
                    "content" => $content,
                    "url" => $url,
                    "date" => $data['date'],
                ];
        });
        echo json_encode([
            "draw" => $draw,
            "recordsTotal" => $count,
            "recordsFiltered" => $count,
            "data" => $datas ?? []
        ]);
    })->setPermissions(['login']);

    $app->router("/users/notification/{active}", 'GET', function($vars) use ($app, $jatbi,$setting) {
        $data = $app->get("notifications","*",["active"=>$app->xss($vars['active']),"deleted"=>0,]);
        $app->update("notifications",["views"=>$data['views']+1],["id"=>$data['id']]);
        if($data['template']=='url'){
            $parsedUrl = parse_url($data['url']);
            $queryExists = isset($parsedUrl['query']);
            if ($queryExists) {
                $geturl = '&views=url';
            } else {
                $geturl = '?views=url';
            }
        }
        header("location: ".$data['url'].$geturl);
    })->setPermissions(['login']);

    $app->router("/users/notification-read", 'POST', function($vars) use ($app, $jatbi,$setting) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $account = $app->get("accounts","*",["id"=>$app->getSession("accounts")['id']]);
        if($account>1){
            $app->update("notifications",["views"=>1],["account"=>$account['id'],"views"=>0]);
            echo json_encode(['status'=>'success','content'=>$jatbi->lang("Cập nhật thành công")]);
        }
        else {
            echo json_encode(['status'=>'error','content'=>$jatbi->lang("Có lỗi xẩy ra")]);
        }
    })->setPermissions(['login']);

    $app->router("/users/notification-deleted", 'GET', function($vars) use ($app, $jatbi) {
        $vars['title'] = $jatbi->lang("Xóa thông báo");
        echo $app->render('templates/common/deleted.html', $vars, 'global');
    })->setPermissions(['login']);

    $app->router("/users/notification-deleted", 'POST', function($vars) use ($app,$jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $boxid = explode(',', $app->xss($_GET['box']));
        $datas = $app->select("notifications","*",["id"=>$boxid,"deleted"=>0]);
        if(count($datas)>0){
            foreach($datas as $data){
                $app->update("notifications",["deleted"=> 1],["id"=>$data['id']]);
            }
            $jatbi->logs('accounts','notifications-deleted',$datas);
            echo json_encode(['status'=>'success',"content"=>$jatbi->lang("Cập nhật thành công")]);
        }
        else {
            echo json_encode(['status'=>'error','content'=>$jatbi->lang("Có lỗi xẩy ra")]);
        }
    })->setPermissions(['login']);

    $app->router("/users/logs", 'GET', function($vars) use ($app, $jatbi,$setting) {
        $vars['router'] = 'logs';
        $vars['title'] = $jatbi->lang("Nhật ký");
        echo $app->render('templates/users/profile.html', $vars);
    })->setPermissions(['login']);

    $app->router("/users/logs", 'POST', function($vars) use ($app, $jatbi) {
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
                "user" => $app->getSession("accounts")['id'],
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
    })->setPermissions(['login']);

    $app->router("/users/logs-views/{id}", 'GET', function($vars) use ($app, $jatbi) {
        $vars['data'] = $app->get("logs","*",["active"=>$vars['id'],"deleted"=>0]);
        if($vars['data']>1){
            echo $app->render('templates/admin/logs-views.html', $vars, 'global');
        }
        else {
            echo $app->render('templates/common/error-modal.html', $vars, 'global');
        }
    })->setPermissions(['login']);

    $app->router("/users/settings", 'GET', function($vars) use ($app, $jatbi,$setting) {
        $vars['router'] = 'settings';
        $vars['title'] = $jatbi->lang("Cài đặt");
        $vars['account'] = $app->get("accounts","*",["id"=>$app->getSession("accounts")['id']]);
        $vars['data'] = $app->get("settings","*",["account"=>$app->getSession("accounts")['id']]);
        echo $app->render('templates/users/profile.html', $vars);
    })->setPermissions(['login']);

    $app->router("/users/settings/{action}", 'POST', function($vars) use ($app,$jatbi,$setting) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $account = $app->get("accounts","*",["id"=>$app->getSession("accounts")['id']]);
        $getsetting = $app->get("settings","*",["account"=>$account['id']]);
        if($account>1){
            if($vars['action']=='notification'){
                $update = [
                    "notification" => $getsetting['notification']==1?0:1,
                ];
                $app->update("settings",$update,["account"=>$account['id']]);
                echo json_encode(['status'=>'success','content'=>$jatbi->lang("Cập nhật thành công")]);
            }
            elseif($vars['action']=='notification-no'){
                $update = [
                    "notification" => 0,
                ];
                $app->update("settings",$update,["account"=>$account['id']]);
                echo json_encode(['status'=>'success','content'=>$jatbi->lang("Cập nhật thành công")]);
            }
            elseif($vars['action']=='notification_mail'){
                $update = [
                    "notification_mail" => $getsetting['notification_mail']==1?0:1,
                ];
                $app->update("settings",$update,["account"=>$account['id']]);
                echo json_encode(['status'=>'success','content'=>$jatbi->lang("Cập nhật thành công")]);
            }
            elseif($vars['action']=='api'){
                $update = [
                    "api" => $getsetting['api']==1?0:1,
                ];
                if($update['api']==0){
                    $update['access_token'] = '';
                }
                else {
                    $update['access_token'] = $app->randomString(128);
                }
                $app->update("settings",$update,["account"=>$account['id']]);
                echo json_encode(['status'=>'success','content'=>$jatbi->lang("Cập nhật thành công")]);
            }
            else {
                echo json_encode(['status'=>'error','content'=>$jatbi->lang("Có lỗi xẩy ra")]);
            }
        }
        else {
            echo json_encode(['status'=>'error','content'=>$jatbi->lang("Có lỗi xẩy ra")]);
        }
    })->setPermissions(['login']);

    $app->router("/users/change-password", 'GET', function($vars) use ($app, $jatbi,$setting) {
        $vars['account'] = $app->get("accounts","*",["id"=>$app->getSession("accounts")['id'],"status"=>"A"]);
        if($vars['account']>1){
            echo $app->render('templates/users/change-password.html', $vars, 'global');
        }
        else {
            echo $app->render('templates/common/error-modal.html', $vars, 'global');
        }
    })->setPermissions(['login']);

    $app->router("/users/change-password", 'POST', function($vars) use ($app, $jatbi,$setting) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $account = $app->get("accounts","*",["id"=>$app->getSession("accounts")['id']]);
        if($account>1){
            if($app->xss($_POST['password-old'])=='' || $app->xss($_POST['password'])=='' || $app->xss($_POST['password-confirm'])==''){
                echo json_encode(['status'=>'error','content'=>$jatbi->lang("Vui lòng không để trống")]);
            }
            elseif($app->xss($_POST['password-confirm'])!=$app->xss($_POST['password'])){
                echo json_encode(['status'=>'error','content'=>$jatbi->lang("Mật khẩu không khớp")]);
            }
            elseif(!password_verify($app->xss($_POST['password-old']), $account['password'])){
                echo json_encode(['status'=>'error','content'=>$jatbi->lang("Mật khẩu không đúng")]);
            }
            if (password_verify($app->xss($_POST['password-old']), $account['password']) && $_POST['password-old'] && $_POST['password']  && $_POST['password-confirm'] && $app->xss($_POST['password-confirm'])==$app->xss($_POST['password'])) {
                $insert = [ 
                    "password" => password_hash($app->xss($_POST['password']), PASSWORD_DEFAULT),
                ];
                $app->update("accounts",$insert,["id"=>$account['id']]);
                echo json_encode(['status'=>'success',"content"=>$jatbi->lang("Thay đổi thành công")]);
                $jatbi->logs('account','change-password',$account);
            }
        }
        else {
            echo json_encode(['status'=>'error','content'=>$jatbi->lang("Có lỗi xẩy ra")]);
        }
    })->setPermissions(['login']);

    $app->router("/users/change-infomation", 'GET', function($vars) use ($app, $jatbi,$setting) {
        $vars['account'] = $app->get("accounts","*",["id"=>$app->getSession("accounts")['id'],"status"=>"A"]);
        if($vars['account']>1){
            echo $app->render('templates/users/change-infomation.html', $vars, 'global');
        }
        else {
            echo $app->render('templates/common/error-modal.html', $vars, 'global');
        }
    })->setPermissions(['login']);

    $app->router("/users/change-infomation", 'POST', function($vars) use ($app, $jatbi,$setting) {
       $app->header([
            'Content-Type' => 'application/json',
        ]);
        $account = $app->get("accounts","*",["id"=>$app->getSession("accounts")['id']]);
        if($account>1){
            if($app->xss($_POST['name'])==''){
                echo json_encode(['status'=>'error','content'=>$jatbi->lang("Vui lòng không để trống")]);
            }
            if ($_POST['name']) {
                $insert = [ 
                    "name"          => $app->xss($_POST['name']),
                    "phone"         => $app->xss($_POST['phone']),
                    "avatar"        => $app->xss($_POST['images']),
                    "birthday"      => date('Y-m-d',strtotime(str_replace('/','-',$_POST['birthday']))),
                    "gender"        => $app->xss($_POST['gender'] ?? 0),
                ];
                $app->update("accounts",$insert,["id"=>$account['id']]);
                echo json_encode(['status'=>'success',"content"=>$jatbi->lang("Thay đổi thành công")]);
                $jatbi->logs('account','change-infomation',$account);
            }
        }
        else {
            echo json_encode(['status'=>'error','content'=>$jatbi->lang("Có lỗi xẩy ra")]);
        }
    })->setPermissions(['login']);

    $app->router("/users/accounts", 'GET', function($vars) use ($app, $jatbi) {
        $vars['title'] = $jatbi->lang("Tài khoản");
        $vars['add'] = '/users/accounts-add';
        $vars['deleted'] = '/users/accounts-deleted';
        $vars['permission'] = $app->select("permissions",["name (text)","id (value)"],["deleted"=>0,"status"=>"A"]);
        echo $app->render('templates/users/accounts.html', $vars);
    })->setPermissions(['accounts']);

    $app->router("/users/accounts", 'POST', function($vars) use ($app, $jatbi) {
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
        $permission = isset($_POST['permission']) ? $_POST['permission'] : '';
        $where = [
            "AND" => [
                "OR" => [
                    "accounts.name[~]" => $searchValue,
                    "accounts.email[~]" => $searchValue,
                    "accounts.account[~]" => $searchValue,
                ],
                "accounts.status[<>]" => $status,
                "accounts.deleted" => 0,
            ],
            "LIMIT" => [$start, $length],
            "ORDER" => [$orderName => strtoupper($orderDir)]
        ];
        if (!empty($permission)) {
            $where["AND"]["accounts.permission"] = $permission;
        }
        $count = $app->count("accounts",[
            "AND" => $where['AND'],
        ]);
        $app->select("accounts", [
                "[>]permissions" => ["permission" => "id"]
            ], 
            [
            'accounts.id',
            'accounts.name',
            'accounts.active',
            'accounts.email',
            'accounts.avatar',
            'accounts.status',
            'permissions.name (permission)',
            ], $where, function ($data) use (&$datas,$jatbi,$app) {
            $datas[] = [
                "checkbox" => $app->component("box",["data"=>$data['active']]),
                "name" => '<img src="/' . $data['avatar'] . '?type=thumb" class="width rounded-circle me-2" style="--width:40px"> '.$data['name'],
                "email" => $data['email'],
                "permission" => $data['permission'],
                "status" => $app->component("status",["url"=>"/users/accounts-status/".$data['active'],"data"=>$data['status'],"permission"=>['accounts.edit']]),
                "action" => $app->component("action",[
                    "button" => [
                        [
                            'type' => 'button',
                            'name' => $jatbi->lang("Sửa"),
                            'permission' => ['accounts.edit'],
                            'action' => ['data-url' => '/users/accounts-edit/'.$data['active'], 'data-action' => 'modal']
                        ],
                        [
                            'type' => 'button',
                            'name' => $jatbi->lang("Xóa"),
                            'permission' => ['accounts.deleted'],
                            'action' => ['data-url' => '/users/accounts-deleted?box='.$data['active'], 'data-action' => 'modal']
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
    })->setPermissions(['accounts']);

    $app->router("/users/accounts-add", 'GET', function($vars) use ($app, $jatbi) {
        $vars['title'] = $jatbi->lang("Thêm Tài khoản");
        $vars['permissions'] = $app->select("permissions","*",["deleted"=>0,"status"=>"A"]);
        $vars['data'] = [
            "status" => 'A',
            "permission" => '',
            "gender" => '',
        ];
        echo $app->render('templates/users/accounts-post.html', $vars, 'global');
    })->setPermissions(['accounts.add']);

    $app->router("/users/accounts-add", 'POST', function($vars) use ($app, $jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        if($app->xss($_POST['name'])=='' || $app->xss($_POST['email'])=='' || $app->xss($_POST['account'])=='' || $app->xss($_POST['password'])=='' || $app->xss($_POST['status'])==''){
            $error = ["status"=>"error","content"=>$jatbi->lang("Vui lòng không để trống")];
        }
        elseif (!filter_var($_POST['email'], FILTER_VALIDATE_EMAIL)) {
            $error = ['status'=>'error','content'=>$jatbi->lang('Email không đúng')];
        }
        if(empty($error)){
            $insert = [
                "type"          => 1,
                "name"          => $app->xss($_POST['name']),
                "account"       => $app->xss($_POST['account']),
                "email"         => $app->xss($_POST['email']),
                "permission"    => $app->xss($_POST['permission']),
                "phone"         => $app->xss($_POST['phone']),
                "gender"        => $app->xss($_POST['gender']),
                "birthday"      => $app->xss($_POST['birthday']),
                "password"      => password_hash($app->xss($_POST['password']), PASSWORD_DEFAULT),
                "active"        => $jatbi->active(),
                "date"          => date('Y-m-d H:i:s'),
                "login"         => 'create',
                "status"        => $app->xss($_POST['status']),
                "lang"          => $_COOKIE['lang'] ?? 'vi',
            ];
            $app->insert("accounts",$insert);
            $getID = $app->id();
            $app->insert("settings",["account"=>$getID]);
            $directory = 'datas/'.$insert['active'];
            mkdir($directory, 0755, true);
            if (isset($_FILES['avatar']) && $_FILES['avatar']['error'] === UPLOAD_ERR_OK) {
                $imageUrl = $_FILES['avatar'];
            }
            else {
                $imageUrl = 'datas/avatar/avatar'.rand(1,10).'.png';
            }
            $handle = $app->upload($imageUrl);
            $path_upload = 'datas/'.$insert['active'].'/images/';
            if (!is_dir($path_upload)) {
                mkdir($path_upload, 0755, true);
            }
            $path_upload_thumb = 'datas/'.$insert['active'].'/images/thumb';
            if (!is_dir($path_upload_thumb)) {
                mkdir($path_upload_thumb, 0755, true);
            }
            $newimages = $jatbi->active();
            if ($handle->uploaded) {
                $handle->allowed        = array('image/*');
                $handle->file_new_name_body = $newimages;
                $handle->Process($path_upload);
                $handle->image_resize   = true;
                $handle->image_ratio_crop  = true;
                $handle->image_y        = '200';
                $handle->image_x        = '200';
                $handle->allowed        = array('image/*');
                $handle->file_new_name_body = $newimages;
                $handle->Process($path_upload_thumb);
            }
            if($handle->processed ){
                $getimage = 'upload/images/'.$newimages;
                $data = [
                    "file_src_name" => $handle->file_src_name,
                    "file_src_name_body" => $handle->file_src_name_body,
                    "file_src_name_ext" => $handle->file_src_name_ext,
                    "file_src_pathname" => $handle->file_src_pathname,
                    "file_src_mime" => $handle->file_src_mime,
                    "file_src_size" => $handle->file_src_size,
                    "image_src_x" => $handle->image_src_x,
                    "image_src_y" => $handle->image_src_y,
                    "image_src_pixels" => $handle->image_src_pixels,
                ];
                $insert = [
                    "account" => $getID,
                    "type" => "images",
                    "content" => $path_upload.$handle->file_dst_name,
                    "date" => date("Y-m-d H:i:s"),
                    "active" => $newimages,
                    "size" => $data['file_src_size'],
                    "data" => json_encode($data),
                ];
                $app->insert("uploads",$insert);
                $app->update("accounts",["avatar"=>$getimage],["id"=>$getID]);
            }
            echo json_encode(['status'=>'success','content'=>$jatbi->lang("Cập nhật thành công"),"test"=>$imageUrl]);
            $jatbi->logs('accounts','accounts-add',$insert);
        }
        else {
            echo json_encode($error);
        }
    })->setPermissions(['accounts.add']);

    $app->router("/users/accounts-edit/{id}", 'GET', function($vars) use ($app, $jatbi) {
        $vars['title'] = $jatbi->lang("Sửa Tài khoản");
        $vars['permissions'] = $app->select("permissions","*",["deleted"=>0,"status"=>"A"]);
        $vars['data'] = $app->get("accounts","*",["active"=>$vars['id'],"deleted"=>0]);
        if($vars['data']>1){
            echo $app->render('templates/users/accounts-post.html', $vars, 'global');
        }
        else {
            echo $app->render('templates/common/error-modal.html', $vars, 'global');
        }
    })->setPermissions(['accounts.edit']);

    $app->router("/users/accounts-edit/{id}", 'POST', function($vars) use ($app, $jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $data = $app->get("accounts","*",["active"=>$vars['id'],"deleted"=>0]);
        if($data>1){
            if($app->xss($_POST['name'])=='' || $app->xss($_POST['email'])=='' || $app->xss($_POST['account'])=='' || $app->xss($_POST['status'])==''){
                $error = ["status"=>"error","content"=>$jatbi->lang("Vui lòng không để trống")];
            }
            elseif (!filter_var($_POST['email'], FILTER_VALIDATE_EMAIL)) {
                $error = ['status'=>'error','content'=>$jatbi->lang('Email không đúng')];
            }
            if(empty($error)){
                $insert = [
                    "type"          => 1,
                    "name"          => $app->xss($_POST['name']),
                    "account"       => $app->xss($_POST['account']),
                    "email"         => $app->xss($_POST['email']),
                    "permission"    => $app->xss($_POST['permission']),
                    "phone"         => $app->xss($_POST['phone']),
                    "gender"        => $app->xss($_POST['gender']),
                    "birthday"      => $app->xss($_POST['birthday']),
                    "password"      => ($_POST['password']==''?$data['password']:password_hash($xss->xss($_POST['password']), PASSWORD_DEFAULT)),
                    "active"        => $data['active'],
                    "date"          => date('Y-m-d H:i:s'),
                    "status"        => $app->xss($_POST['status']),
                    "lang"          => $data['lang'] ?? 'vi',
                ];
                $app->update("accounts",$insert,["id"=>$data['id']]);
                if($_FILES['avatar']){
                    $imageUrl = $_FILES['avatar'];
                    $handle = $app->upload($imageUrl);
                    $path_upload = 'datas/'.$insert['active'].'/images/';
                    if (!is_dir($path_upload)) {
                        mkdir($path_upload, 0755, true);
                    }
                    $path_upload_thumb = 'datas/'.$insert['active'].'/images/thumb';
                    if (!is_dir($path_upload_thumb)) {
                        mkdir($path_upload_thumb, 0755, true);
                    }
                    $newimages = $jatbi->active();
                    if ($handle->uploaded) {
                        $handle->allowed        = array('image/*');
                        $handle->file_new_name_body = $newimages;
                        $handle->Process($path_upload);
                        $handle->image_resize   = true;
                        $handle->image_ratio_crop  = true;
                        $handle->image_y        = '200';
                        $handle->image_x        = '200';
                        $handle->allowed        = array('image/*');
                        $handle->file_new_name_body = $newimages;
                        $handle->Process($path_upload_thumb);
                    }
                    if($handle->processed ){
                        $getimage = 'upload/images/'.$newimages;
                        $data = [
                            "file_src_name" => $handle->file_src_name,
                            "file_src_name_body" => $handle->file_src_name_body,
                            "file_src_name_ext" => $handle->file_src_name_ext,
                            "file_src_pathname" => $handle->file_src_pathname,
                            "file_src_mime" => $handle->file_src_mime,
                            "file_src_size" => $handle->file_src_size,
                            "image_src_x" => $handle->image_src_x,
                            "image_src_y" => $handle->image_src_y,
                            "image_src_pixels" => $handle->image_src_pixels,
                        ];
                        $insert = [
                            "account" => $getID,
                            "type" => "images",
                            "content" => $path_upload.$handle->file_dst_name,
                            "date" => date("Y-m-d H:i:s"),
                            "active" => $newimages,
                            "size" => $data['file_src_size'],
                            "data" => json_encode($data),
                        ];
                        $app->insert("uploads",$insert);
                        $app->update("accounts",["avatar"=>$getimage],["id"=>$data['id']]);
                    }
                }
                echo json_encode(['status'=>'success','content'=>$jatbi->lang("Cập nhật thành công")]);
                $jatbi->logs('accounts','accounts-edit',$insert);
            }
            else {
                echo json_encode($error);
            }
        }
        else {
            echo json_encode(["status"=>"error","content"=>$jatbi->lang("Không tìm thấy dữ liệu")]);
        }
    })->setPermissions(['accounts.edit']);

    $app->router("/users/accounts-status/{id}", 'POST', function($vars) use ($app, $jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $data = $app->get("accounts","*",["active"=>$vars['id'],"deleted"=>0]);
        if($data>1){
            if($data>1){
                if($data['status']==='A'){
                    $status = "D";
                } 
                elseif($data['status']==='D'){
                    $status = "A";
                }
                $app->update("accounts",["status"=>$status],["id"=>$data['id']]);
                $jatbi->logs('accounts','accounts-status',$data);
                echo json_encode(['status'=>'success','content'=>$jatbi->lang("Cập nhật thành công")]);
            }
            else {
                echo json_encode(['status'=>'error','content'=>$jatbi->lang("Cập nhật thất bại"),]);
            }
        }
        else {
            echo json_encode(["status"=>"error","content"=>$jatbi->lang("Không tìm thấy dữ liệu")]);
        }
    })->setPermissions(['accounts.edit']);

    $app->router("/users/accounts-deleted", 'GET', function($vars) use ($app, $jatbi) {
        $vars['title'] = $jatbi->lang("Xóa Tài khoản");
        echo $app->render('templates/common/deleted.html', $vars, 'global');
    })->setPermissions(['accounts.deleted']);

    $app->router("/users/accounts-deleted", 'POST', function($vars) use ($app,$jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $boxid = explode(',', $app->xss($_GET['box']));
        $datas = $app->select("accounts","*",["active"=>$boxid,"deleted"=>0]);
        if(count($datas)>0){
            foreach($datas as $data){
                $app->update("accounts",["deleted"=> 1],["id"=>$data['id']]);
                $name[] = $data['name'];
            }
            $jatbi->logs('accounts','accounts-deleted',$datas);
            $jatbi->trash('/users/accounts-restore',"Tài khoản: ".implode(', ',$name),["database"=>'accounts',"data"=>$boxid]);
            echo json_encode(['status'=>'success',"content"=>$jatbi->lang("Cập nhật thành công")]);
        }
        else {
            echo json_encode(['status'=>'error','content'=>$jatbi->lang("Có lỗi xẩy ra")]);
        }
    })->setPermissions(['accounts.deleted']);

    $app->router("/users/accounts-restore/{id}", 'GET', function($vars) use ($app, $jatbi) {
        $vars['data'] = $app->get("trashs","*",["active"=>$vars['id'],"deleted"=>0]);
        if($vars['data']>1){
            echo $app->render('templates/common/restore.html', $vars, 'global');
        }
        else {
            echo $app->render('templates/common/error-modal.html', $vars, 'global');
        }
    })->setPermissions(['accounts.deleted']);

    $app->router("/users/accounts-restore/{id}", 'POST', function($vars) use ($app,$jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $trash = $app->get("trashs","*",["active"=>$vars['id'],"deleted"=>0]);
        if($trash>1){
            $datas = json_decode($trash['data']);
            foreach($datas->data as $active) {
                $app->update("accounts",["deleted"=>0],["active"=>$active]);
            }
            $app->delete("trashs",["id"=>$trash['id']]);
            $jatbi->logs('accounts','accounts-restore',$datas);
            echo json_encode(['status'=>'success',"content"=>$jatbi->lang("Cập nhật thành công")]);
        }
        else {
            echo json_encode(['status'=>'error','content'=>$jatbi->lang("Có lỗi xẩy ra")]);
        }
    })->setPermissions(['accounts.deleted']);

    $app->router("/users/permission", 'GET', function($vars) use ($app, $jatbi) {
        $vars['title'] = $jatbi->lang("Nhóm quyền");
        echo $app->render('templates/users/permission.html', $vars);
    })->setPermissions(['permission']);

    $app->router("/users/permission", 'POST', function($vars) use ($app, $jatbi) {
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
                    "name[~]" => $searchValue,
                ],
                "status[<>]" => $status,
                "deleted" => 0,
            ],
            "LIMIT" => [$start, $length],
            "ORDER" => [$orderName => strtoupper($orderDir)]
        ];
        $count = $app->count("permissions",["AND" => $where['AND']]);
        $app->select("permissions", "*", $where, function ($data) use (&$datas,$jatbi,$app) {
            $datas[] = [
                "checkbox" => $app->component("box",["data"=>$data['active']]),
                "name" => $data['name'],
                "status" => $app->component("status",["url"=>"/users/permission-status/".$data['active'],"data"=>$data['status'],"permission"=>['permission.edit']]),
                "action" => $app->component("action",[
                    "button" => [
                        [
                            'type' => 'button',
                            'name' => $jatbi->lang("Sửa"),
                            'permission' => ['permission.edit'],
                            'action' => ['data-url' => '/users/permission-edit/'.$data['active'], 'data-action' => 'modal']
                        ],
                        [
                            'type' => 'button',
                            'name' => $jatbi->lang("Xóa"),
                            'permission' => ['permission.deleted'],
                            'action' => ['data-url' => '/users/permission-deleted?box='.$data['active'], 'data-action' => 'modal']
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
    })->setPermissions(['permission']);

    $app->router("/users/permission-add", 'GET', function($vars) use ($app, $jatbi,$permission) {
        $vars['title'] = $jatbi->lang("Thêm Nhóm Quyền");
        $vars['permissions'] = $permission;
        $vars['data'] = [
            "status" => 'A',
        ];
        echo $app->render('templates/users/permission-post.html', $vars, 'global');
    })->setPermissions(['permission.add']);

    $app->router("/users/permission-add", 'POST', function($vars) use ($app, $jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        if($app->xss($_POST['name'])=='' || $app->xss($_POST['status'])==''){
            $error = ["status"=>"error","content"=>$jatbi->lang("Vui lòng không để trống")];
        }
        if(empty($error)){
            foreach($_POST['permissions'] as $key => $per){
                $permission[$key] = $per;
            }
            $insert = [
                "name"          => $app->xss($_POST['name']),
                "status"        => $app->xss($_POST['status']),
                "active"        => $jatbi->active(),
                "permissions"   => json_encode($permission),
            ];
            $app->insert("permissions",$insert);
            echo json_encode(['status'=>'success','content'=>$jatbi->lang("Cập nhật thành công")]);
            $jatbi->logs('permission','permission-add',$insert);
        }
        else {
            echo json_encode($error);
        }
    })->setPermissions(['permission.add']);

    $app->router("/users/permission-edit/{id}", 'GET', function($vars) use ($app, $jatbi,$permission) {
        $vars['title'] = $jatbi->lang("Sửa Nhóm Quyền");
        $vars['data'] = $app->get("permissions","*",["active"=>$vars['id'],"deleted"=>0]);
        if($vars['data']>1){
            $vars['permissions'] = $permission;
            $vars['checkper'] = json_decode($vars['data']['permissions'],true);
            echo $app->render('templates/users/permission-post.html', $vars, 'global');
        }
        else {
            echo $app->render('templates/common/error-modal.html', $vars, 'global');
        }
    })->setPermissions(['permission.edit']);

    $app->router("/users/permission-edit/{id}", 'POST', function($vars) use ($app, $jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $data = $app->get("permissions","*",["active"=>$vars['id'],"deleted"=>0]);
        if($data>1){
            if($app->xss($_POST['name'])=='' || $app->xss($_POST['status'])==''){
                $error = ["status"=>"error","content"=>$jatbi->lang("Vui lòng không để trống")];
            }
            if(empty($error)){
                foreach($_POST['permissions'] as $key => $per){
                    $permission[$key] = $per;
                }
                $insert = [
                    "name"          => $app->xss($_POST['name']),
                    "status"        => $app->xss($_POST['status']),
                    "permissions"   => json_encode($permission),
                ];
                $app->update("permissions",$insert,["id"=>$data['id']]);
                echo json_encode(['status'=>'success','content'=>$jatbi->lang("Cập nhật thành công")]);
                $jatbi->logs('permission','permission-edit',$insert);
            }
            else {
                echo json_encode($error);
            }
        }
        else {
            echo json_encode(["status"=>"error","content"=>$jatbi->lang("Không tìm thấy dữ liệu")]);
        }
    })->setPermissions(['permission.edit']);

    $app->router("/users/permission-status/{id}", 'POST', function($vars) use ($app, $jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $data = $app->get("permissions","*",["active"=>$vars['id'],"deleted"=>0]);
        if($data>1){
            if($data>1){
                if($data['status']==='A'){
                    $status = "D";
                } 
                elseif($data['status']==='D'){
                    $status = "A";
                }
                $app->update("permissions",["status"=>$status],["id"=>$data['id']]);
                $jatbi->logs('permission','permission-status',$data);
                echo json_encode(['status'=>'success','content'=>$jatbi->lang("Cập nhật thành công")]);
            }
            else {
                echo json_encode(['status'=>'error','content'=>$jatbi->lang("Cập nhật thất bại"),]);
            }
        }
        else {
            echo json_encode(["status"=>"error","content"=>$jatbi->lang("Không tìm thấy dữ liệu")]);
        }
    })->setPermissions(['permission.edit']);

    $app->router("/users/permission-deleted", 'GET', function($vars) use ($app, $jatbi) {
        $vars['title'] = $jatbi->lang("Xóa Tài khoản");
        echo $app->render('templates/common/deleted.html', $vars, 'global');
    })->setPermissions(['permission.deleted']);

    $app->router("/users/permission-deleted", 'POST', function($vars) use ($app,$jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $boxid = explode(',', $app->xss($_GET['box']));
        $datas = $app->select("permissions","*",["active"=>$boxid,"deleted"=>0]);
        if(count($datas)>0){
            foreach($datas as $data){
                $app->update("permissions",["deleted"=> 1],["id"=>$data['id']]);
                $name[] = $data['name'];
            }
            $jatbi->logs('permission','permission-deleted',$datas);
            $jatbi->trash('/users/permission-restore',"Nhóm quyền: ".implode(', ',$name),["database"=>'permission',"data"=>$boxid]);
            echo json_encode(['status'=>'success',"content"=>$jatbi->lang("Cập nhật thành công")]);
        }
        else {
            echo json_encode(['status'=>'error','content'=>$jatbi->lang("Có lỗi xẩy ra")]);
        }
    })->setPermissions(['permission.deleted']);

    $app->router("/users/permission-restore/{id}", 'GET', function($vars) use ($app, $jatbi) {
        $vars['data'] = $app->get("trashs","*",["active"=>$vars['id'],"deleted"=>0]);
        if($vars['data']>1){
            echo $app->render('templates/common/restore.html', $vars, 'global');
        }
        else {
            echo $app->render('templates/common/error-modal.html', $vars, 'global');
        }
    })->setPermissions(['permission.deleted']);

    $app->router("/users/permission-restore/{id}", 'POST', function($vars) use ($app,$jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $trash = $app->get("trashs","*",["active"=>$vars['id'],"deleted"=>0]);
        if($trash>1){
            $datas = json_decode($trash['data']);
            foreach($datas->data as $active) {
                $app->update("permissions",["deleted"=>0],["active"=>$active]);
            }
            $app->delete("trashs",["id"=>$trash['id']]);
            $jatbi->logs('permission','permission-restore',$datas);
            echo json_encode(['status'=>'success',"content"=>$jatbi->lang("Cập nhật thành công")]);
        }
        else {
            echo json_encode(['status'=>'error','content'=>$jatbi->lang("Có lỗi xẩy ra")]);
        }
    })->setPermissions(['permission.deleted']);
?>