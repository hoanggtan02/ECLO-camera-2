## Yêu cầu
- PHP 8.1+
## Cài đặt
```bash
$ composer require eclo/app
```

## Cấu hình

```php
// Require Composer's autoloader.
require 'vendor/autoload.php';

// sử dụng ECLO namespace.
use ECLO\App;
$app = new App();

// hoặc sử dụng với database

use ECLO\App;

$dbConfig = [
	// [required]
	'type' => 'mysql',
	'host' => 'localhost',
	'database' => '.......',
	'username' => '.......',
	'password' => '.......',
	// [optional]
	'charset' => 'utf8mb4',
	'collation' => 'utf8mb4_general_ci',
	'port' => 3306,
	'prefix' => '',
	'logging' => true,
	'error' => PDO::ERRMODE_SILENT,
	'option' => [
		PDO::ATTR_CASE => PDO::CASE_NATURAL
	],
	'command' => [
		'SET SQL_MODE=ANSI_QUOTES'
	]
];
$app = new App($dbConfig);
$app->run();
```
Thiết lặp router
```php
// gọi router đến 1 file
 $app->request("/home",'home.php');
$app->group("/admin", function($app){
   $app->router("/config", 'GET', function($vars) use($app){
     echo 'test';
   });
});
// cấu hình router ['GET','POST']
$app->router("/home", 'GET', function($vars) {
    $hello = 'Hello ECLO';
    echo $hello;
    if($app->method()==='GET'){
      echo 'GET';
    }
    // render đến giao diện
    $vars['hello'] = $hello;
    echo $app->render('templates/test.html', $vars);
});

$app->run();

```
Thiết lặp router với ID 
```php
$app->router("/home/{id}", 'GET', function($vars) {
    $hello = 'Hello '.$vard['id'];
    echo $hello;
});
```
Thiết lặp router với POST
```php
$app->router("/home", 'POST', function($vars) {
    $hello = 'Hello '.$vard['id'];
    echo $hello;
});

```
Thiết lặp file gốc và đăng ký component
```php
// set dữ liệu
$eclo = [];
$app->setValueData('eclo', $eclo);


// sử dụng file chính
$app->setGlobalFile(__DIR__ . '/global.php');

// đăng ký component
$app->setComponent('header', function($vars) {
   echo "<header><h1>Header Component</h1></header>";
});
$app->setComponent('footer', function($vars) {
   echo "<footer><p>Footer Component</p></footer>";
});
$app->setComponent('text', function($vars) {
   include('text.html');
});

// hiện thỉ component trong file global.php

echo $app->component('header');
require_once $templatePath;
echo $app->component('footer')

```

Sử dụng router với app
```php
$app->router("/home",  'GET', function($vars) use ($app) {
   // set header cho router
   $app->header([
    'Content-Type' => 'application/json',
   ]);
   $response = [
     'message' => 'This is the home page.',
     'data' => $vars
   ];
   echo json_encode($response);
});
```
Sử dụng router với app có database 

```php
$app->router("/home",  'GET', function($vars) use ($app) {
   $app->header([
    'Content-Type' => 'application/json',
   ]);
   $datas = $app->select("table","*",["email"=>"info@eclo.vn"]);
   echo json_encode($datas);
});
```
Set quyền truy cập cho router

```php
$userPermissions = ["home","home.add"];
// set quyền truy cập
$app->setUserPermissions($userPermissions);
$app->router("/home", 'GET', function($vars) {
    $hello = 'Hello '.$vard['id'];
    echo $hello;
})->setPermissions(["home"]);
```
Sử lý chống XSS
```php
$input = '<script>alert("XSS")</script>';
$safeInput = $app->xss($input);
echo $safeInput;

```
Thiết lặp cookie

```php
// Đặt cookie:
$app->setCookie('username', 'JohnDoe');
// Lấy cookie:
$username = $app->getCookie('username');
// Xóa cookie
$app->deleteCookie('username');
```

Thiết lặp SESSION

```php
// Đặt Session:
$app->setSession('user_id', 123);
// Lấy Session:
$username = $app->getSession('username');
// Xóa Session
$app->deleteSession('username');
```

Xử lý với database
```php

$data = $app->select("account", [
	"user_name",
	"email"
], [
	"user_id[>]" => 100
]);
 
// $data = array(
//  [0] => array(
//	  "user_name" => "foo",
//	  "email" => "foo@bar.com"
//  ),
//  [1] => array(
//	  "user_name" => "cat",
//	  "email" => "cat@dog.com"
//  )
// )
 
foreach($data as $item) {
	echo "user_name:" . $item["user_name"] . " - email:" . $item["email"] . "<br/>";
}

// insert databse
$app->insert("account", [
	"user_name" => "foo",
	"email" => "foo@bar.com",
	"age" => 25
]);
 
$account_id = $app->id();

// cập nhật database

$app->update("account", [
	"type" => "user",
], [
	"user_id[<]" => 1000
]);

// xóa database
$app->delete("account", [
   "AND" => [
	"type" => "business",
	"age[<]" => 18
   ]
]);

// get database

$email = $app->get("account", "email", [
	"user_id" => 1234
]);
```

Xử lý API

```php
//Gửi yêu cầu GET đến API:
$response = $app->apiGet('https://api.example.com/data', ['Authorization: Bearer your_token']);
echo $response;

//Gửi yêu cầu POST đến API:
$response = $app->apiPost('https://api.example.com/data', ['key1' => 'value1', 'key2' => 'value2'], ['Authorization: Bearer your_token']);
echo $response;

// Gửi yêu cầu PUT đến API:
$response = $app->apiPut('https://api.example.com/data/1', ['key1' => 'new_value'], ['Authorization: Bearer your_token']);
echo $response;

// Gửi yêu cầu DELETE đến API:
$response = $app->apiDelete('https://api.example.com/data/1', ['Authorization: Bearer your_token']);
echo $response;

```

Xử lý JWT 

```php
// đăng ký JWT
$app->JWT('test', 'HS256');

// Tạo JWT
$payload = ['user_id' => 123, 'exp' => time() + 3600]; // Ví dụ payload
$token = $app->addJWT($payload);
echo "JWT: " . $token;

// Giải mã JWT
$decoded = $app->decodeJWT($token,new stdClass());
if ($decoded) {
    echo "Decoded: " . print_r($decoded, true);
} else {
    echo "Invalid token";
}

// Xác thực JWT
if ($app->validateJWT($token)) {
    echo "Token is valid";
} else {
    echo "Token is invalid";
}
```
Xử lý Gửi mail

```php
// đăng ký mail
$mail = $app->Mail([
    'username' => 'info@eclo.vn',
    'password' => 'avmy hoql znpf nssa',
    'from_email' => 'info@eclo.vn',
    'from_name' => 'Eclo',
    'host' => 'smtp.gmail.com',
    'port' => 465,
    'encryption' => 'smtp', // Có thể thay đổi thành 'tls' nếu cần
]);

try {
    // Đặt người nhận email
    $mail->addAddress('jatbirat@gmail.com', 'Recipient Name'); 
    // Nội dung email
    $mail->isHTML(true);                                  // Đặt định dạng email là HTML
    $mail->Subject = 'Đây là tiêu đề của email';
    $mail->Body    = 'Nội dung của email <b>in bold!</b>';
    $mail->AltBody = 'Nội dung không HTML';

    // Gửi email
    if($mail->send()) {
	echo 'Message has been sent';
    } else {
	echo 'Message could not be sent. Mailer Error: ' . $mail->ErrorInfo;
    }
} catch (Exception $e) {
    echo 'Message could not be sent. Mailer Error: ' . $mail->ErrorInfo;
}
```
Khác 
```php
// chuyển trang
$app->redirect('https://example.com');

// quay lại trang
$app->back();

//Tạo số ngẫu nhiên:
$randomNumber = $app->randomNumber(6); // 123456

//Tạo chuỗi ký tự ngẫu nhiên:
$randomString = $app->randomString(5); // ABCDE

// Format URL:
$seoUrl = $app->formatUrl('Hello World! This is a test.');

// Cắt chuỗi:
$truncated = $app->truncateString('This is a long string that needs to be truncated.', 20);

// Cắt ký tự:
$cut = $app->cutCharacters('abcdefghij', 2, 5);

//Kiểm tra địa chỉ IP:
$isValid = $app->isValidIp('192.168.1.1');

//Kiểm tra thiết bị:
$deviceType = $app->checkDevice();

//Chặn một IP cụ thể:
$app->blockIp('192.168.1.100');

//Chặn nhiều IP:
$app->blockIp('192.168.1.100')->blockIp('10.0.0.1');

// Lấy thời gian hiện tại:
$currentTime = $app->currentTime();


//Định dạng ngày:
$formattedDate = $app->formatDate(time(), 'd/m/Y');

//Thêm thời gian:
$newTime = $app->addTime(time(), 'P1D', 'd/m/Y H:i:s'); // Thêm 1 ngày

// Tính hiệu số thời gian:
$diff = $app->diffTime(strtotime('2024-01-01'), time());

// nén file css và js

$app->minifyCSS(['/path/to/file1.css', '/path/to/file2.css'], '/path/to/output.min.css');
$app->minifyJS(['/path/to/file1.js', '/path/to/file2.js'], '/path/to/output.min.js');

$app->minifyAndGzipJS(['/path/to/file1.js'], '/path/to/output.min.js.gz');


```
