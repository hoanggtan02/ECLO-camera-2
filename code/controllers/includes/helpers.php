<?php 
	if (!defined('ECLO')) die("Hacking attempt");
	class Jatbi {
		protected $app;
	    public function __construct($app) {
	        $this->app = $app;
	    }
	    public function lang($key) {
		    global $lang;
		    return isset($lang[$key]) ? $lang[$key] : $key;
		}
		public function formatResponse($response) {
		    $response = htmlspecialchars($response, ENT_QUOTES, 'UTF-8');
		    if (substr_count($response, '```') % 2 != 0) {
		        $response .= "\n```";
		    }
		    $pattern = '/```(\w*)\n([\s\S]*?)```/';
		    $replacement = '<pre><code class="language-$1">$2</code></pre>';
		    $formattedResponse = preg_replace($pattern, $replacement, $response);
		    $formattedResponse = nl2br($formattedResponse);
		    return $formattedResponse;
		}
		public function active() {
		    $uuid = '';
		    for ($i = 0; $i < 8; $i++) {
		        $uuid .= dechex(mt_rand(0, 15));
		    }
		    $uuid .= '-';
		    for ($i = 0; $i < 4; $i++) {
		        $uuid .= dechex(mt_rand(0, 15));
		    }
		    $uuid .= '-4';
		    for ($i = 0; $i < 3; $i++) {
		        $uuid .= dechex(mt_rand(0, 15));
		    }
		    $uuid .= '-';
		    $uuid .= dechex(mt_rand(8, 11));
		    for ($i = 0; $i < 3; $i++) {
		        $uuid .= dechex(mt_rand(0, 15));
		    }
		    $uuid .= '-';
		    for ($i = 0; $i < 12; $i++) {
		        $uuid .= dechex(mt_rand(0, 15));
		    }
		    return $uuid;
		}
		public function account(){
			$checkuser = $this->app->getSession("accounts");
		    if($checkuser){
		    	$getuser = $this->app->get("accounts","*" ,["id"=>$checkuser['id']]);
		    }
		    return $getuser;
		}
	    public function checkAuthenticated($requests) {
	        if ($this->app->getCookie('token') && empty($this->app->getSession("accounts"))) {
		        $decoded = $this->app->decodeJWT($this->app->getCookie('token'), '');
		        if ($decoded) {
		            $accounts_login = $this->app->get("accounts_login","*",[
		                "accounts"  => $this->app->get("accounts","id",["active"=>$decoded->id]),
		                "token"     => $decoded->token,
		                "agent"     => $decoded->agent,
		                "deleted"   => 0,
		                "ORDER"     => [
		                    "id"    => "DESC",  
		                ]
		            ]);
		            if($accounts_login>1){
		                $checkUser = $this->app->get("accounts","*",["id"=>$accounts_login['accounts'],"status"=>"A","deleted"=>0]);
		                if($checkUser>1){
		                	$gettoken = $this->app->randomString(256);
			                $payload = [
			                    "ip"        => $this->app->xss($_SERVER['REMOTE_ADDR']),
			                    "id"        => $checkUser['active'],
			                    "email"     => $checkUser['email'],
			                    "token"     => $gettoken,
			                    "agent"     => $_SERVER["HTTP_USER_AGENT"],
			                ];
			                $token = $this->app->addJWT($payload);
			                $getLogins = $this->app->get("accounts_login","*",[
			                    "accounts"  => $checkUser['id'],
			                    "agent"     => $payload['agent'],
			                    "deleted"   => 0,
			                ]);
			                if($getLogins>1){
			                    $this->app->update("accounts_login",[
			                        "accounts" => $checkUser['id'],
			                        "ip"    =>  $payload['ip'],
			                        "token" =>  $payload['token'],
			                        "agent" =>  $payload["agent"],
			                        "date"  => date("Y-m-d H:i:s"),
			                    ],["id"=>$getLogins['id']]);
			                }
			                else {
			                    $this->app->insert("accounts_login",[
			                        "accounts" => $checkUser['id'],
			                        "ip"    =>  $payload['ip'],
			                        "token" =>  $payload['token'],
			                        "agent" =>  $payload["agent"],
			                        "date"  => date("Y-m-d H:i:s"),
			                    ]);
			                }
		                    $this->app->setSession('accounts',[
		                        "id" 		=> $checkUser['id'],
		                        "agent"     => $payload['agent'],
		                        "token"     => $payload['token'],
		                        "active" 	=> $checkUser['active'],
		                    ]);		
		                    $this->app->setCookie('token', $token,time()+((3600 * 24 * 30)*12),'/');                
		                }
		                else {
		                	$this->app->deleteSession('accounts');
		    				$this->app->deleteCookie('token');
		                }
		            }
		        }
		    }
		    $checkuser = $this->app->getSession("accounts");
		    if($checkuser){
		    	$getuser = $this->app->get("accounts",["id","type","permission"],["deleted"=>0,"status"=>'A',"id"=>$checkuser['id']]);
		    	if($getuser['id']>0){
		    		$getPermission = $this->app->get("permissions",["id","permissions"],[
		    			"deleted"=>0,
		    			"status"=>'A',
		    			"id"=>$getuser['permission']
		    		]);
			    	$getLogins = $this->app->get("accounts_login","id",[
	                    "accounts"  => $getuser['id'],
			            "token"     => $checkuser['token'],
	                    "agent"     => $checkuser['agent'],
	                    "deleted"   => 0,
	                ]);
	                if($getLogins>0 && $getPermission>1){
		    			$setPermission = ["login" => 'login'];
	                	$getPermission = json_decode($getPermission['permissions'],true);
	                	if (is_array($getPermission)) {
						    $setPermission = array_merge($setPermission, $getPermission);
						}
	                	$this->app->setUserPermissions($setPermission);
	                	foreach ($requests as $key => $menus) {
						    $main_names[$key]["name"] = $menus['name'];
						    $main_names[$key]["item"] = [];
						    foreach ($menus['item'] as $key_item => $item) {
					            $main_names[$key]['items'][$key_item]["menu"] = $item['menu'];
					            $main_names[$key]['items'][$key_item]["url"] = $item['url'];
					            $main_names[$key]['items'][$key_item]["main"] = $item['main'];
					            $main_names[$key]['items'][$key_item]["icon"] = $item['icon'];
						        if (!empty($item['sub'])) {
						            foreach ($item['sub'] as $sub_key => $subs) {
						                if (!empty($setPermission) && isset($setPermission[$sub_key])) {
						                    $main_names[$key]['items'][$key_item]["sub"][$sub_key] = $subs;
						                }
						            }
						            if (empty($main_names[$key]['items'][$key_item]["sub"])) {
						                unset($main_names[$key]['items'][$key_item]);
						            }
						        }
						    }
						}

	                	$this->app->setValueData('menu', $main_names);
	                }
			        else {
			        	$this->app->deleteSession('accounts');
			    		$this->app->deleteCookie('token');
			        }
		    	}
		        else {
		        	$this->app->deleteSession('accounts');
		    		$this->app->deleteCookie('token');
		        }
		    }
	    }
	    public function permission($permissions) {
		    $checkuser = $this->app->getSession("accounts");
		    if ($checkuser) {
		        $getuser = $this->app->get("accounts", ["id", "type", "permission"], [
		            "deleted" => 0,
		            "status" => 'A',
		            "id" => $checkuser['id']
		        ]);
		        if (!empty($getuser['id'])) {
		            $getPermission = $this->app->get("permissions", ["id", "permissions"], [
		                "deleted" => 0,
		                "status" => 'A',
		                "id" => $getuser['permission']
		            ]);
		            $userPermissions = !empty($getPermission['permissions']) ? json_decode($getPermission['permissions'], true) : [];
		            if (empty($userPermissions)) {
		                return false;
		            }
		            if (empty($permissions)) {
		                return true;
		            }
		            return (bool) array_intersect((array) $permissions, $userPermissions);
		        }
		    }
		    return false;
		}
		public function notification($user,$account,$title,$body,$click_action,$template=null,$type=null,$data=null){
			global $setting;
			if($template==''){
				$template = 'url';
			}
			$insert = [
				"user" => $user,
				"account" => $account,
				"title" => $title,
				"content" => $body,
				"url" => $click_action,
				"date" => date("Y-m-d H:i:s"),
				"template" => $template,
				"active" => $this->active(),
				"type"=>  $type ?? 'content',
				"data" => $data,
			];
			$this->app->insert("notifications",$insert);
			// $getsetting = $this->app->get("settings","*",["account"=>$account]);
			// if($getsetting['notification']==1){
				// $cmd = 'php /www/wwwroot/ellm.io/dev/run/notification.php ' . escapeshellarg(json_encode($insert));
                // exec($cmd . ' > /dev/null 2>&1 &', $output, $return_var);
			// }
		}
		public function logs($dispatch,$action,$content,$account = null){
			$ip = $_SERVER['REMOTE_ADDR'];
		    if (!empty($_SERVER['HTTP_CLIENT_IP'])) {
		        $ip = $_SERVER['HTTP_CLIENT_IP'];
		    } else if (!empty($_SERVER['HTTP_X_FORWARDED_FOR'])) {
		        $ip = $_SERVER['HTTP_X_FORWARDED_FOR'];
		    }
			$this->app->insert("logs",[
				"user" 		=> $account ?? $this->app->getSession('accounts')['id'],
				"dispatch" 	=> $dispatch,
				"action" 	=> $action,
				"date" 		=> date('Y-m-d H:i:s'),
				"url" 		=> 'http://'.$_SERVER["SERVER_NAME"].$_SERVER["REQUEST_URI"],
				"ip" 		=> $ip,
				"active"    => $this->active(),
				"browsers"	=> $_SERVER["HTTP_USER_AGENT"] ?? '',
	            "content"   => json_encode($content),
			]);
		}
		public function trash($router,$content,$data){
			$ip = $_SERVER['REMOTE_ADDR'];
		    if (!empty($_SERVER['HTTP_CLIENT_IP'])) {
		        $ip = $_SERVER['HTTP_CLIENT_IP'];
		    } else if (!empty($_SERVER['HTTP_X_FORWARDED_FOR'])) {
		        $ip = $_SERVER['HTTP_X_FORWARDED_FOR'];
		    }
			$this->app->insert("trashs",[
				"account" 	=> $account ?? $this->app->getSession('accounts')['id'],
				"content" 		=> $content,
				"router"    => $router,
				"data"		=> json_encode($data),
				"date" 		=> date('Y-m-d H:i:s'),
				"url" 		=> 'http://'.$_SERVER["SERVER_NAME"].$_SERVER["REQUEST_URI"],
				"ip" 		=> $ip,
				"active"    => $this->active(),
			]);
		}
	    public function pages_ajax($count, $limit, $page, $class = null, $last = null) {
		    global $view, $lang, $router, $detect;
		    $total = ceil($count / $limit);
		    $return = null;
		    $url = $_SERVER['REQUEST_URI'];
		    $urlParts = parse_url($url);
		    parse_str($urlParts['query'] ?? '', $queryParams);
		    if ($page < $total) {
		        $queryParams['pg'] = $page + 1;
		    } else {
		        unset($queryParams['pg']);
		    }
		    $queryString = http_build_query($queryParams);
		    $return .= '<div class="'.$class.'">';
		    $return .= '<div class="pagination text-center w-100">';
		    $getlast = '';
		    
		    if ($last) {
		        $getlast = '&last='.$last;
		    }
		    
		    if ($total > 1) {
		        if ($page != $total) {
		            $return .= '<a href="'.$urlParts['path'].'?'.$queryString.$getlast.'" class="page-link next pjax-load btn border-0 bg-light text-dark mx-auto">Xem thêm</a>';
		        }
		    }
		    $return .= '</div>';
		    $return .= '</div>';
		    return $return;
		}
	    public function pages($count,$limit,$page,$name=null){
	        global $view,$lang,$router,$detect;
	        $total = ceil($count/$limit);
	        $return = null;
	        $getpage = null;
	        $name = $name==''?'&pg':$name;
	        $return .= '<ul class="pagination">';
	        if($total>1){
	            $url = $_SERVER['REQUEST_URI'];
	            if($_SERVER['QUERY_STRING']==''){
	                $view = $url.'?';
	            } else {
	                $view = '?'.$_SERVER['QUERY_STRING'].'';
	            }
	            $view = preg_replace("#(/?|&)".$name."=([0-9]{1,})#","",$view);
	            if($page!=1){
	            	$return .= '<li class="page-item mx-1"><a href="'.$view.$name.'=1" class="page-link rounded-3 bg-opacity-10 bg-secondary border-0" data-pjax >&laquo;&laquo;</a></li>';
	                $return .= '<li class="page-item mx-1 d-none d-md-block"><a href="'.$view.$name.'='.($page-1).'" class="page-link rounded-3 bg-opacity-10 bg-secondary border-0" data-pjax >&laquo;</a></li>';
	            }
	            for ($number=1; $number<=$total; $number++) { 
	                if($page>4 && $number==1 || $page<$total-1 && $number==$total){
	                    $return .= '<li class="page-item mx-1 d-none d-md-block"><a href="#" class="page-link rounded-3 bg-opacity-10 bg-secondary border-0 page-link-hide">...</a><li>';
	                }
	                if($number<$page+4 && $number>$page-4){
	                    $return .= '<li class="page-item mx-1"><a href="'.$view.$name.'='.$number.'" class="page-link rounded-3 bg-'.($page==$number?'primary text-light':'secondary bg-opacity-10').' border-0" data-pjax >'.$number.'</a></li>';
	                }
	                $getnumber = $number;
	            }
	            if($page!=$total){
	                $return .= '<li class="page-item mx-1 d-none d-md-block"><a href="'.$view.$name.'='.($page+1).'" class="page-link rounded-3 bg-opacity-10 bg-secondary border-0" data-pjax >&raquo;</a></li>';
	                $return .= '<li class="page-item mx-1"><a href="'.$view.$name.'='.$total.'" class="page-link rounded-3 bg-opacity-10 bg-secondary border-0" data-pjax >&raquo;&raquo;</a></li>';
	            }
	        }
	        $return .= '</ul>';
	        return $return;
	    }
	}
?>