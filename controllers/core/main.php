<?php
	if (!defined('ECLO')) die("Hacking attempt");
    $jatbi = new Jatbi($app);
    $setting = $app->getValueData('setting');
    $app->router("/",'GET', function($vars) use ($app,$jatbi,$setting) {
        if(!$app->getSession("accounts")){
            $vars['templates'] = 'login';
            echo $app->render('templates/login.html', $vars);
        }
        else {
            echo $app->render('templates/home.html', $vars);
        }
    });
    $app->router("/login", 'GET', function($vars) use ($app, $jatbi,$setting) {
        if(!$app->getSession("accounts")){
            $vars['templates'] = 'login';
            echo $app->render('templates/login.html', $vars);
        }
        else {
            $app->redirect('/');
        }
    });
    $app->router("/login", 'POST', function($vars) use ($app, $jatbi,$setting) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        if($app->xss($_POST['email']) && $app->xss($_POST['password'])){
            $data = $app->get("accounts","*",[
                "OR"=>[
                    "email"     => $app->xss($_POST['email']),
                    "account"   => $app->xss($_POST['email']),
                ],
                "status"=>"A",
                "deleted"=>0
            ]);
            if(isset($data) && password_verify($app->xss($_POST['password']), $data['password'])) {
                $gettoken = $app->randomString(256);
                $payload = [
                    "ip"        => $app->xss($_SERVER['REMOTE_ADDR']),
                    "id"        => $data['active'],
                    "email"     => $data['email'],
                    "token"     => $gettoken,
                    "agent"     => $_SERVER["HTTP_USER_AGENT"],
                ];
                $token = $app->addJWT($payload);
                $getLogins = $app->get("accounts_login","*",[
                    "accounts"  => $data['id'],
                    "agent"     => $payload['agent'],
                    "deleted"   => 0,
                ]);
                if($getLogins>1){
                    $app->update("accounts_login",[
                        "accounts" => $data['id'],
                        "ip"    =>  $payload['ip'],
                        "token" =>  $payload['token'],
                        "agent" =>  $payload["agent"],
                        "date"  => date("Y-m-d H:i:s"),
                    ],["id"=>$getLogins['id']]);
                }
                else {
                    $app->insert("accounts_login",[
                        "accounts" => $data['id'],
                        "ip"    =>  $payload['ip'],
                        "token" =>  $payload['token'],
                        "agent" =>  $payload["agent"],
                        "date"  => date("Y-m-d H:i:s"),
                    ]);
                }
                $app->setSession('accounts',[
                    "id" => $data['id'],
                    "agent" => $payload['agent'],
                    "token" => $payload['token'],
                    "active" => $data['active'],
                ]);
                if($app->xss($_POST['remember'] ?? '' )){
                    $app->setCookie('token', $token,time()+$setting['cookie'],'/');
                }
                echo json_encode(['status' => 'success','content' => $jatbi->lang('Đăng nhập thành công')]);
                $payload['did'] = $app->getCookie('did');
                $jatbi->logs('accounts','login',$payload);
            }
            else {
                echo json_encode(['status' => 'error','content' => $jatbi->lang('Tài khoản hoặc mật khẩu không đúng')]);
            }
        }
        else {
            echo json_encode(['status' => 'error','content' => $jatbi->lang('Vui lòng không để trống')]);
        }
    });
    $app->router("/logout", 'GET', function($vars) use ($app) {
        $app->deleteSession('accounts');
        $app->deleteCookie('token');
        $app->redirect('/');
    });
    $app->router("/register", 'GET', function($vars) use ($app, $jatbi,$setting) {
        if(!$app->getSession("accounts")){
            $vars['templates'] = 'register';
            echo $app->render('templates/login.html', $vars);
        }
        else {
            $app->redirect('/');
        }
    });
    $app->router("/register", 'POST', function($vars) use ($app, $jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $checkaccount = $app->get("accounts", ["email","date_deleted","deleted","status"],["email"=>$app->xss($_POST['email']),"ORDER"=>["id"=>"DESC"]]);
        $getcode = $app->get("account_code",["code","id"],["email"=>$app->xss($_POST['email']),"type"=>'register',"status"=>0,"date[>=]"=>date("Y-m-d H:i:s",strtotime("-5 minute")),"ORDER"=>["id"=>"DESC"]]);
        $date_deleted = strtotime(date("Y-m-d H:i:s",strtotime($checkaccount['date_deleted']. ' +7 days')));
        $date_now = strtotime(date("Y-m-d H:i:s"));
        if($app->xss($_POST['name'])=='' || $app->xss($_POST['email'])=='' || $app->xss($_POST['password'])=='' || $app->xss($_POST['password-comfirm'])=='' || $app->xss($_POST['email-comfirm'])=='' ){
            $error = ['status'=>'error','content'=>$jatbi->lang('Vui lòng không để trống')];
        }
        elseif($app->xss($_POST['password-comfirm'])!=$app->xss($_POST['password'])){
            $error = ['status'=>'error','content'=>$jatbi->lang('Mật khẩu không khớp')];
        }
        elseif (!filter_var($_POST['email'], FILTER_VALIDATE_EMAIL)) {
            $error = ['status'=>'error','content'=>$jatbi->lang('Email không đúng')];
        }
        elseif($app->xss($_POST['email']) == $checkaccount['email'] && $checkaccount['deleted']==0 && $checkaccount['status']=='A'){
            $error = ['status'=>'error','content'=>$jatbi->lang('Tài khoản đã có người sử dụng')];
        }
        elseif($checkaccount['status']=='D'){
            $error = ['status'=>'error','content'=>$jatbi->lang('Email này đã bị vô hiệu hóa. Vui lòng liên hệ bộ phần kỹ thuật của ELLM')];
        }
        elseif($date_now<$date_deleted && $checkaccount['deleted']==1){
            $error = ['status'=>'error','content'=>$jatbi->lang('Email này đã bị vô hiệu hóa. Vui lòng đợi sau 7 ngày để đăng ký mới')];
        }
        elseif($getcode['code']!=$app->xss($_POST['email-comfirm'])){
            $error = ['status'=>'error','content'=>$jatbi->lang('Mã xác thực không đúng')];
        }
        if (empty($error)) {
            $createuid =  $jatbi->generateRandomNumbers(12);
            $getuid = $app->get("accounts","id",["uid"=>$createuid]);
            if($getuid>0){
                $uid = $createuid.'1';
            }
            else {
                $uid = $createuid;
            }
            if($app->getCookie('invite-code')){
                $getinvite = $app->get("accounts","id",["invite_code"=>$app->xss($app->getCookie('invite-code')),"deleted"=>0,"status"=>'A']);
                if($getinvite>0){
                    $invite_code = $getinvite;
                }
            }
            $insert = [
                "uid"           => $uid,
                "name"          => $app->xss($_POST['name']),
                "email"         => $app->xss($_POST['email']),
                "password"      => password_hash($app->xss($_POST['password']), PASSWORD_DEFAULT),
                "type"          => 2,
                "active"        => $jatbi->active(),
                "avatar"        => 'no-image',
                "date"          => date('Y-m-d H:i:s'),
                "login"         => 'register',
                "status"        => 'A',
                "invite"        => $invite_code ?? 0,
                "invite_code"   => $jatbi->generateRandomNumbers(9),
                "lang"          => $_COOKIE['lang'] ?? 'vi',
            ];
            $app->insert("accounts",$insert);
            $getID = $app->id();
            $app->insert("settings",["account"=>$getID]);
            $directory = 'datas/'.$insert['active'];
            mkdir($directory, 0755, true);
            $imageUrl = 'images/accounts/avatar'.rand(1,10).'.png';
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
            $packages = [
                "account"   => $getID,
                "price"     => 2000,
                "total"     => 2000,
                "watermark" => 1,
                "api"       => 1,
                "date"      => $insert['date'],
            ];
            $app->insert("packages",$packages);
            $gettoken = $app->randomString(256);
            $payload = [
                "ip"        => $app->xss($_SERVER['REMOTE_ADDR']),
                "id"        => $insert['active'],
                "email"     => $insert['email'],
                "token"     => $gettoken,
                "agent"     => $_SERVER["HTTP_USER_AGENT"],
            ];
            $token = $app->addJWT($payload);
            $getLogins = $app->get("accounts_login","*",[
                "accounts"  => $getID,
                "agent"     => $payload['agent'],
                "deleted"   => 0,
            ]);
            $app->insert("accounts_login",[
                "accounts" => $getID,
                "ip"    =>  $payload['ip'],
                "token" =>  $payload['token'],
                "agent" =>  $payload["agent"],
                "date"  => date("Y-m-d H:i:s"),
            ]);
            $app->setSession('accounts',[
                "id" => $getID,
                "agent" => $payload['agent'],
                "token" => $payload['token'],
                "active" => $insert['active'],
            ]);
            $app->update("account_code",["status"=>1],["id"=>$getcode['id']]);
            $app->setCookie('token', $token);
            $jatbi->notification($getID,$getID,'Chào mừng','Chào mừng bạn đến với ELLM','/action/text/welcome','modal-url');
            if($insert['invite']>0){
                $jatbi->notification($insert['invite'],$insert['invite'],'Cảm ơn','Cám ơn bạn đã giới thiệu bạn bè của mình cho ELLM.','/action/text/thanks','modal-url');
            }
            $app->deleteCookie('invite-code');
            $jatbi->logs('account','register',$payload);
            echo json_encode(['status' => 'success','content' => $jatbi->lang('Đăng nhập thành công'),'load'=>"true"]);
        }
        else {
            echo json_encode($error);
        }
    });
    $app->router("/email-comfirm", 'POST', function($vars) use ($app, $jatbi) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        if($app->xss($_POST['email'])==''){
            echo json_encode(['status' => 'error','content' => $jatbi->lang('Vui lòng không để trống')]);
        }
        elseif (!filter_var($app->xss($_POST['email']), FILTER_VALIDATE_EMAIL)) {
            echo json_encode(['status'=>'error','content'=>$jatbi->lang('Email không đúng')]);
        }
        if($app->xss($_POST['email'] && filter_var($app->xss($_POST['email']), FILTER_VALIDATE_EMAIL))){
            $checkaccount = $app->count("accounts", "id",["email"=>$app->xss($_POST['email']),"deleted"=>0]);
            if($checkaccount>0){
                echo json_encode(['status'=>'error','content'=>$jatbi->lang("Tài khoản đã có người sử dụng")]);
            }
            else {
                $code = substr(str_shuffle("0123456789"), 0, 6);
                try {
                    $mail = $app->Mail([
                        'username' => 'info@ellm.io',
                        'password' => 'obhf udlq gyhp ptwn',
                        'from_email' => 'info@ellm.io',
                        'from_name' => 'ELLM',
                        'host' => 'smtp.gmail.com',
                        'port' => 465,
                        'encryption' => 'smtp',
                    ]);
                    $mail->setFrom('info@ellm.io','No-reply');
                    $mail->addAddress($app->xss($_POST['email']));
                    $mail->CharSet = "utf-8";
                    $mail->isHTML(true);
                    $mail->Subject = $jatbi->lang('ELLM - Mã Xác nhận đăng ký');
                    $mail->Body    = '<div style="padding: 0 19px">
                        <h1>'.$jatbi->lang("Xin chào").'</h1>
                        <h2>'.$jatbi->lang("Chào mừng bạn đến với ELLM").'</h2>
                        <p>'.$jatbi->lang("Mã xác nhận để đăng ký tài khoản của bạn là").': <strong>'.$code.'</strong></p>
                        <p>'.$jatbi->lang("Vui lòng không cung cấp cho người khác.").'</p>
                        <p>'.$jatbi->lang("Cảm ơn bạn đã chọn ELLM làm đối tác để đạt được mục tiêu của mình. Chúng tôi mong được hợp tác với bạn và giúp bạn thành công.").'</p>
                    </div>';
                    $mail->send();
                    echo json_encode(['status'=>'success','content'=>$jatbi->lang("Đã gửi mã xác thực. Vui lòng kiểm tra email của bạn")]);
                    $app->insert("account_code",["email"=>$app->xss($_POST['email']),"code"=>$code,"date"=>date("Y-m-d H:i:s"),"type"=>'register']);
                } catch (Exception $e) {
                    echo $jatbi->lang("Có lỗi xảy ra vui lòng thử lại");
                }
            }
        }
    });
    $app->router("/forgot-password", 'GET', function($vars) use ($app, $jatbi) {
        if(!$app->getSession("accounts")){
            $vars['templates'] = 'forgot';
            echo $app->render('templates/login.html', $vars);
        }
        else {
            $app->redirect('/');
        }
    });
    $app->router("/lang/{active}", 'GET', function($vars) use ($app, $jatbi,$setting) {
        $app->setCookie('lang', $vars['active'],time()+$setting['cookie'],'/');
        $account = $app->get("accounts","id",["id"=>$app->getSession("accounts")['id']]);
        if($account>0){
            $app->update("accounts",["lang"=>$vars['active']],["id"=>$account] );
        }
        $app->redirect($_SERVER['HTTP_REFERER']);
    });
    $app->router("/login-check/google", 'GET', function($vars) use ($app,$jatbi,$setting) {
         $app->header([
            'Content-Type' => 'application/json',
        ]);
        if (isset($_GET['code'])){
            $gettoken = $google->fetchAccessTokenWithAuthCode($_GET['code']);
            if(isset($gettoken['error'])){
                header('Location: /?error='.$jatbi->lang("Có lỗi xẩy ra"));
                exit;
            }
            $google->setAccessToken($gettoken);
            $google_oauth = new Google_Service_Oauth2($google);
            $user_info = $google_oauth->userinfo->get();
            $data = $app->get("accounts","*",["email"=>trim($user_info->email),"deleted"=>0,"status"=>"A"]);
            if($data>1){
                $gettoken = $app->randomString(256);
                $payload = [
                    "ip"        => $app->xss($_SERVER['REMOTE_ADDR']),
                    "id"        => $data['active'],
                    "email"     => $data['email'],
                    "token"     => $gettoken,
                    "agent"     => $_SERVER["HTTP_USER_AGENT"],
                ];
                $token = $app->addJWT($payload);
                $getLogins = $app->get("accounts_login","*",[
                    "accounts"  => $data['id'],
                    "agent"     => $payload['agent'],
                    "deleted"   => 0,
                ]);
                if($getLogins>1){
                    $app->update("accounts_login",[
                        "accounts" => $data['id'],
                        "ip"    =>  $payload['ip'],
                        "token" =>  $payload['token'],
                        "agent" =>  $payload["agent"],
                        "date"  => date("Y-m-d H:i:s"),
                    ],["id"=>$getLogins['id']]);
                }
                else {
                    $app->insert("accounts_login",[
                        "accounts" => $data['id'],
                        "ip"    =>  $payload['ip'],
                        "token" =>  $payload['token'],
                        "agent" =>  $payload["agent"],
                        "date"  => date("Y-m-d H:i:s"),
                    ]);
                }
                if($data['uid']==0){
                    $createuid =  $jatbi->generateRandomNumbers(12);
                    $getuid = $app->get("accounts","id",["uid"=>$createuid]);
                    if($getuid>0){
                        $uid = $createuid.'1';
                    }
                    else {
                        $uid = $createuid;
                    }
                    $app->update("accounts",["uid"=>$uid],["id"=>$data['id']]);
                }
                $app->setSession('accounts',[
                    "id" => $data['id'],
                    "agent" => $payload['agent'],
                    "token" => $payload['token'],
                    "active" => $data['active'],
                ]);
                if($app->xss($_POST['remember'] ?? '' )){
                    $app->setCookie('token', $token,time()+$setting['cookie'],'/');
                }
                echo json_encode(['status' => 'success','content' => $jatbi->lang('Đăng nhập thành công'),'load'=>"true"]);
                $jatbi->logs('account','login',$payload);
                if($data['avatar']=='no-image' || $data['avatar']=='' || $data['avatar']=='no-image.png'){
                    $imageUrl = 'images/accounts/avatar'.rand(1,10).'.png';
                    $handle = $app->upload($imageUrl);
                    $path_upload = 'datas/'.$data['active'].'/images/';
                    if (!is_dir($path_upload)) {
                        mkdir($path_upload, 0755, true);
                    }
                    $path_upload_thumb = 'datas/'.$data['active'].'/images/thumb';
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
                        $getdata = [
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
                        $upload = [
                            "account" => $data['id'],
                            "type" => "images",
                            "content" => $path_upload.$handle->file_dst_name,
                            "date" => date("Y-m-d H:i:s"),
                            "active" => $newimages,
                            "size" => $getdata['file_src_size'],
                            "data" => json_encode($getdata),
                        ];
                        $app->insert("uploads",$upload);
                        $app->update("accounts",["avatar"=>$getimage],["id"=>$data['id']]);
                    }
                }
                if($data['login_data']==''){
                    $app->update("accounts",["login_data" => $app->xss($user_info->id),],["id"=>$data['id']]);
                }
                $getpackages = $app->get("packages","*",["account"=>$data['id']]);
                if($getpackages['total']==0 ){
                    $packages = [
                        "account"   => $data['id'],
                        "price"     => 1000,
                        "total"     => 1000,
                        "watermark" => 1,
                        "api"       => 1,
                        "date"      => $insert['date'],
                    ];
                    $app->update("packages",$packages,['id'=>$getpackages['id']]);
                }
                header("location: /");
            }
            else {
                $createuid =  $jatbi->generateRandomNumbers(12);
                $getuid = $app->get("accounts","id",["uid"=>$createuid]);
                if($getuid>0){
                    $uid = $createuid.'1';
                }
                else {
                    $uid = $createuid;
                }
                if($app->getCookie('invite-code')){
                    $getinvite = $app->get("accounts","id",["invite_code"=>$app->xss($app->getCookie('invite-code')),"deleted"=>0,"status"=>'A']);
                    if($getinvite>0){
                        $invite_code = $getinvite;
                    }
                }
                $insert = [
                    "uid"           => $uid,
                    "name"          => $app->xss($user_info->name),
                    "email"         => $app->xss($user_info->email),
                    "password"      => password_hash($app->xss($createuid), PASSWORD_DEFAULT),
                    "type"          => 2,
                    "active"        => $jatbi->active(),
                    "avatar"        => 'no-image',
                    "date"          => date('Y-m-d H:i:s'),
                    "login"         => 'google',
                    "status"        => 'A',
                    "login_data"    => $app->xss($user_info->id),
                    "invite"        => $invite_code ?? 0,
                    "invite_code"   => $jatbi->generateRandomNumbers(9),
                    "lang"          => $_COOKIE['lang'] ?? 'vi',
                ];
                $app->insert("accounts",$insert);
                $getID = $app->id();
                $app->insert("settings",["account"=>$getID]);
                $directory = 'datas/'.$insert['active'];
                mkdir($directory, 0755, true);
                $imageUrl = 'images/accounts/avatar'.rand(1,10).'.png';
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
                $packages = [
                    "account"   => $getID,
                    "price"     => 2000,
                    "total"     => 2000,
                    "watermark" => 1,
                    "api"       => 1,
                    "date"      => $insert['date'],
                ];
                $app->insert("packages",$packages);
                $gettoken = $app->randomString(256);
                $payload = [
                    "ip"        => $app->xss($_SERVER['REMOTE_ADDR']),
                    "id"        => $insert['active'],
                    "email"     => $insert['email'],
                    "token"     => $gettoken,
                    "agent"     => $_SERVER["HTTP_USER_AGENT"],
                ];
                $token = $app->addJWT($payload);
                $getLogins = $app->get("accounts_login","*",[
                    "accounts"  => $getID,
                    "agent"     => $payload['agent'],
                    "deleted"   => 0,
                ]);
                $app->insert("accounts_login",[
                    "accounts" => $getID,
                    "ip"    =>  $payload['ip'],
                    "token" =>  $payload['token'],
                    "agent" =>  $payload["agent"],
                    "date"  => date("Y-m-d H:i:s"),
                ]);
                $app->setSession('accounts',[
                    "id" => $getID,
                    "agent" => $payload['agent'],
                    "token" => $payload['token'],
                    "active" => $insert['active'],
                ]);
                $app->update("account_code",["status"=>1],["id"=>$getcode['id']]);
                $app->setCookie('token', $token);
                $jatbi->notification($getID,$getID,'Chào mừng','Chào mừng bạn đến với ELLM','/action/text/welcome','modal-url');
                if($insert['invite']>0){
                    $jatbi->notification($insert['invite'],$insert['invite'],'Cảm ơn','Cám ơn bạn đã giới thiệu bạn bè của mình cho ELLM.','/action/text/thanks','modal-url');
                }
                $app->deleteCookie('invite-code');
                $jatbi->logs('account','register',$payload);
                header("location: /");
            }
        }
        else {
            header("HTTP/1.0 404 Not Found");
            die();
        }
    });
    $app->router("/login-check/apple", 'POST', function($vars) use ($app,$jatbi,$setting) {
         $app->header([
            'Content-Type' => 'application/json',
        ]);
        $teamId = 'R5PDZ36U72';
        $keyId = 'U846T9K9U7';
        $sub = 'io.ellm.login';
        $aud = 'https://appleid.apple.com'; // it's a fixed URL value
        $iat = strtotime('now');
        $exp = strtotime('+60days');
        $keyContent = file_get_contents('controllers/includes/applekey.txt');
        $apple_secret = $app->addJWT([
            'iss' => $teamId,
            'iat' => $iat,
            'exp' => $exp,
            'aud' => $aud,
            'sub' => $sub,
        ], $keyContent, 'ES256', $keyId);
        // $apple_secret = JWT::encode();
        function http($url, $params=false) {
          $ch = curl_init($url);
          curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
          if($params)
            curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($params));
            curl_setopt($ch, CURLOPT_HTTPHEADER, [
            'Accept: application/json',
            'User-Agent: curl', # Apple requires a user agent header at the token endpoint
          ]);
          $response = curl_exec($ch);
          return json_decode($response);
        }
        if (isset($_POST['code'])){
            $response = http('https://appleid.apple.com/auth/token', [
                'grant_type' => 'authorization_code',
                'code' => $_POST['code'],
                'redirect_uri' => $setting['apple_redirect_uri'],
                'client_id' => $setting['apple_id'],
                'client_secret' => $apple_secret,
            ]);
            // echo print_r($response);
            if(!isset($response->access_token)) {
                header('Location: /');
            }
            $claims = explode('.', $response->id_token)[1];
            $claims = json_decode(base64_decode($claims));
            $user_info = json_decode(json_encode($claims));
            if($user_info->email_verified=='true'){
                $getname = explode("@",$user_info->email);
                $data = $app->get("accounts","*",["email"=>trim($user_info->email),"deleted"=>0,"status"=>"A"]);
                if($data>1){
                    $gettoken = $app->randomString(256);
                    $payload = [
                        "ip"        => $app->xss($_SERVER['REMOTE_ADDR']),
                        "id"        => $data['active'],
                        "email"     => $data['email'],
                        "token"     => $gettoken,
                        "agent"     => $_SERVER["HTTP_USER_AGENT"],
                    ];
                    $token = $app->addJWT($payload);
                    $getLogins = $app->get("accounts_login","*",[
                        "accounts"  => $data['id'],
                        "agent"     => $payload['agent'],
                        "deleted"   => 0,
                    ]);
                    if($getLogins>1){
                        $app->update("accounts_login",[
                            "accounts" => $data['id'],
                            "ip"    =>  $payload['ip'],
                            "token" =>  $payload['token'],
                            "agent" =>  $payload["agent"],
                            "date"  => date("Y-m-d H:i:s"),
                        ],["id"=>$getLogins['id']]);
                    }
                    else {
                        $app->insert("accounts_login",[
                            "accounts" => $data['id'],
                            "ip"    =>  $payload['ip'],
                            "token" =>  $payload['token'],
                            "agent" =>  $payload["agent"],
                            "date"  => date("Y-m-d H:i:s"),
                        ]);
                    }
                    if($data['uid']==0){
                        $createuid =  $jatbi->generateRandomNumbers(12);
                        $getuid = $app->get("accounts","id",["uid"=>$createuid]);
                        if($getuid>0){
                            $uid = $createuid.'1';
                        }
                        else {
                            $uid = $createuid;
                        }
                        $app->update("accounts",["uid"=>$uid],["id"=>$data['id']]);
                    }
                    $app->setSession('accounts',[
                        "id" => $data['id'],
                        "agent" => $payload['agent'],
                        "token" => $payload['token'],
                        "active" => $data['active'],
                    ]);
                    if($app->xss($_POST['remember'] ?? '' )){
                        $app->setCookie('token', $token,time()+$setting['cookie'],'/');
                    }
                    echo json_encode(['status' => 'success','content' => $jatbi->lang('Đăng nhập thành công'),'load'=>"true"]);
                    $jatbi->logs('account','login',$payload);
                    if($data['avatar']=='no-image' || $data['avatar']=='' || $data['avatar']=='no-image.png'){
                        $imageUrl = 'images/accounts/avatar'.rand(1,10).'.png';
                        $handle = $app->upload($imageUrl);
                        $path_upload = 'datas/'.$data['active'].'/images/';
                        if (!is_dir($path_upload)) {
                            mkdir($path_upload, 0755, true);
                        }
                        $path_upload_thumb = 'datas/'.$data['active'].'/images/thumb';
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
                            $getdata = [
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
                            $upload = [
                                "account" => $data['id'],
                                "type" => "images",
                                "content" => $path_upload.$handle->file_dst_name,
                                "date" => date("Y-m-d H:i:s"),
                                "active" => $newimages,
                                "size" => $getdata['file_src_size'],
                                "data" => json_encode($getdata),
                            ];
                            $app->insert("uploads",$upload);
                            $app->update("accounts",["avatar"=>$getimage],["id"=>$data['id']]);
                        }
                    }
                    if($data['login_data']==''){
                        $app->update("accounts",["login_data" => $app->xss($user_info->id),],["id"=>$data['id']]);
                    }
                    $getpackages = $app->get("packages","*",["account"=>$data['id']]);
                    if($getpackages['total']==0 ){
                        $packages = [
                            "account"   => $data['id'],
                            "price"     => 1000,
                            "total"     => 1000,
                            "watermark" => 1,
                            "api"       => 1,
                            "date"      => $insert['date'],
                        ];
                        $app->update("packages",$packages,['id'=>$getpackages['id']]);
                    }
                    header("location: /");
                }
                else {
                    $createuid =  $jatbi->generateRandomNumbers(12);
                    $getuid = $app->get("accounts","id",["uid"=>$createuid]);
                    if($getuid>0){
                        $uid = $createuid.'1';
                    }
                    else {
                        $uid = $createuid;
                    }
                    if($app->getCookie('invite-code')){
                        $getinvite = $app->get("accounts","id",["invite_code"=>$app->xss($app->getCookie('invite-code')),"deleted"=>0,"status"=>'A']);
                        if($getinvite>0){
                            $invite_code = $getinvite;
                        }
                    }
                    $insert = [
                        "uid"           => $uid,
                        "name"          => $app->xss($getname[0]),
                        "email"         => $app->xss($user_info->email),
                        "password"      => password_hash($app->xss($createuid), PASSWORD_DEFAULT),
                        "type"          => 2,
                        "active"        => $jatbi->active(),
                        "avatar"        => 'no-image',
                        "date"          => date('Y-m-d H:i:s'),
                        "login"         => 'apple',
                        "status"        => 'A',
                        "login_data"    => $app->xss($user_info->email),
                        "invite"        => $invite_code ?? 0,
                        "invite_code"   => $jatbi->generateRandomNumbers(9),
                        "lang"          => $_COOKIE['lang'] ?? 'vi',
                    ];
                    $app->insert("accounts",$insert);
                    $getID = $app->id();
                    $app->insert("settings",["account"=>$getID]);
                    $directory = 'datas/'.$insert['active'];
                    mkdir($directory, 0755, true);
                    $imageUrl = 'images/accounts/avatar'.rand(1,10).'.png';
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
                    $packages = [
                        "account"   => $getID,
                        "price"     => 2000,
                        "total"     => 2000,
                        "watermark" => 1,
                        "api"       => 1,
                        "date"      => $insert['date'],
                    ];
                    $app->insert("packages",$packages);
                    $gettoken = $app->randomString(256);
                    $payload = [
                        "ip"        => $app->xss($_SERVER['REMOTE_ADDR']),
                        "id"        => $insert['active'],
                        "email"     => $insert['email'],
                        "token"     => $gettoken,
                        "agent"     => $_SERVER["HTTP_USER_AGENT"],
                    ];
                    $token = $app->addJWT($payload);
                    $getLogins = $app->get("accounts_login","*",[
                        "accounts"  => $getID,
                        "agent"     => $payload['agent'],
                        "deleted"   => 0,
                    ]);
                    $app->insert("accounts_login",[
                        "accounts" => $getID,
                        "ip"    =>  $payload['ip'],
                        "token" =>  $payload['token'],
                        "agent" =>  $payload["agent"],
                        "date"  => date("Y-m-d H:i:s"),
                    ]);
                    $app->setSession('accounts',[
                        "id" => $getID,
                        "agent" => $payload['agent'],
                        "token" => $payload['token'],
                        "active" => $insert['active'],
                    ]);
                    $app->update("account_code",["status"=>1],["id"=>$getcode['id']]);
                    $app->setCookie('token', $token);
                    $jatbi->notification($getID,$getID,'Chào mừng','Chào mừng bạn đến với ELLM','/action/text/welcome','modal-url');
                    if($insert['invite']>0){
                        $jatbi->notification($insert['invite'],$insert['invite'],'Cảm ơn','Cám ơn bạn đã giới thiệu bạn bè của mình cho ELLM.','/action/text/thanks','modal-url');
                    }
                    $app->deleteCookie('invite-code');
                    $jatbi->logs('account','register',$payload);
                    header("location: /");
                }
            }
        }
        else {
            header("HTTP/1.0 404 Not Found");
            die();
        }
    });

    $app->router("/upload/{path}/{id}", 'GET', function($vars) use ($app,$jatbi) {
        ob_start();
        $getType = $app->xss($_GET['type'] ?? '');
        $data = $app->get("uploads",'content',["active"=>$vars['id'],"deleted"=>0]);
        if($getType=='thumb'){
            $file_path = str_replace($vars['path'],$vars['path'].'/thumb',$data);
        }
        else {
            $file_path = $data;
        }
        $path = '/'.$file_path;
        if (!file_exists($file_path)) {
            $path = '/templates/assets/img/logo-small.svg';
        }
        $mime_type = mime_content_type($path);
        header('Content-Type: ' . $mime_type);
        header('Cache-Control: public, max-age=31536000, immutable');
        header('X-Accel-Redirect: '.$path);
        ob_end_flush();
    });

    $app->router("/upload-file", 'POST', function($vars) use ($app,$jatbi,$setting) {
        $app->header([
            'Content-Type' => 'application/json',
        ]);
        $account = $app->get("accounts","*",["id"=>$app->getSession("accounts")['id']]);
        $handle = $app->upload($_FILES['file']);
        $path_upload = 'datas/'.$account['active'].'/images/';
        if (!is_dir($path_upload)) {
            mkdir($path_upload, 0755, true);
        }
        $path_upload_thumb = 'datas/'.$account['active'].'/images/thumb';
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
                "account" => $account['id'],
                "type" => "images",
                "content" => $path_upload.$handle->file_dst_name,
                "date" => date("Y-m-d H:i:s"),
                "active" => $newimages,
                "size" => $data['file_src_size'],
                "mime" => $data['file_src_mime'],
                "data" => json_encode($data),
            ];
            $app->insert("uploads",$insert);
        }
        echo json_encode(['status'=>"success","url"=>$getimage]);
    })->setPermissions(['login']);

    $app->router("::error",'GET', function($vars) use ($app,$jatbi,$setting) {
        if(!empty($_SERVER['HTTP_X_REQUESTED_WITH']) && strtolower($_SERVER['HTTP_X_REQUESTED_WITH']) === 'xmlhttprequest'){
            echo $app->render('templates/common/error-modal.html', $vars, 'global');
        }
        else {
            echo $app->render('templates/error.html', $vars);
        }
    });
?>