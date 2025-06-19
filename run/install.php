<?php
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);
if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    // Tải file đã upload
    $appName = htmlspecialchars($_POST['app_name']);
    $appIdentifier = htmlspecialchars($_POST['app_identifier']);
    $websiteURL = htmlspecialchars($_POST['website_url']);
    $backgroundColor = htmlspecialchars($_POST['background_color']);
    $themeColor = htmlspecialchars($_POST['theme_color']);
    $DB_TYPE = htmlspecialchars($_POST['DB_TYPE']);
    $DB_HOST = htmlspecialchars($_POST['DB_HOST']);
    $DB_PORT = htmlspecialchars($_POST['DB_PORT']);
    $DB_DATABASE = htmlspecialchars($_POST['DB_DATABASE']);
    $DB_USERNAME = htmlspecialchars($_POST['DB_USERNAME']);
    $DB_PASSWORD = htmlspecialchars($_POST['DB_PASSWORD']);
    $DB_CHARSET = htmlspecialchars($_POST['DB_CHARSET']);
    $DB_COLLATION = htmlspecialchars($_POST['DB_COLLATION']);
    $DB_PREFIX = htmlspecialchars($_POST['DB_PREFIX']);
    $iconFile = $_FILES['icon'];
    // Kiểm tra tệp upload
    // if ($iconFile['error'] === UPLOAD_ERR_OK) {
        // $uploadDir = '../assest-src/';
        // if (!is_dir($uploadDir)) {
        //     mkdir($uploadDir, 0755, true);
        // }
        // $uploadedFile = $uploadDir . basename($iconFile['name']);
        
        // $imageInfo = getimagesize($iconFile['tmp_name']);
        // if ($imageInfo[0] > 1024 || $imageInfo[1] > 1024) {
        //     die('Hình ảnh phải có kích thước 1024x1024.');
        // }
        // $splashHtml .= '<meta name="mobile-web-app-capable" content="yes" />' . PHP_EOL;
        // $splashHtml .= '<meta name="apple-touch-fullscreen" content="yes" />' . PHP_EOL;
        // $splashHtml .= '<meta name="apple-mobile-web-app-title" content="'.$appName.'" />' . PHP_EOL;
        // $splashHtml .= '<meta name="apple-mobile-web-app-capable" content="yes" />' . PHP_EOL;
        // $splashHtml .= '<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent" />' . PHP_EOL;
        // $splashHtml .= '<meta name="theme-color" media="(prefers-color-scheme: light)" content="'.$themeColor.'" />' . PHP_EOL;
        // $splashHtml .= '<meta name="theme-color" media="(prefers-color-scheme: dark)" content="'.$themeColor.'" />' . PHP_EOL;
        // Di chuyển file đã upload và xử lý các kích thước khác
        // if (move_uploaded_file($iconFile['tmp_name'], $uploadedFile)) {
            // $sizes = [128, 144, 152, 192, 256, 512];
            // $iconPaths = [];
            // $splashHtml .= '<link rel="icon" type="image/png" href="/assest-src/512x512.png"/>' . PHP_EOL;
            // foreach ($sizes as $size) {
            //     $resizedFile = $uploadDir . "{$size}x{$size}.png";
            //     $iconPaths[] = ["src" => "/assest-src/{$size}x{$size}.png", "sizes" => "{$size}x{$size}", "type" => "image/png"];

            //     // Tạo ảnh đã resize
            //     $srcImage = imagecreatefrompng($uploadedFile);
            //     $dstImage = imagecreatetruecolor($size, $size);

            //     // Giữ nền trong suốt (với hình ảnh PNG)
            //     imagesavealpha($dstImage, true);
            //     $transparent = imagecolorallocatealpha($dstImage, 0, 0, 0, 127); // Nền trong suốt
            //     imagefill($dstImage, 0, 0, $transparent);

            //     // Resize ảnh
            //     imagecopyresampled($dstImage, $srcImage, 0, 0, 0, 0, $size, $size, $imageInfo[0], $imageInfo[1]);

            //     imagepng($dstImage, $resizedFile);
            //     imagedestroy($dstImage);
            //     $splashHtml .= '<link rel="apple-touch-icon" sizes="'.$size.'x'.$size.'" href="/assest-src/'.$size.'x'.$size.'.png">' . PHP_EOL;
            // }
            // imagedestroy($srcImage);

            // Tạo file manifest.json
            $manifest = [
                "name" => $appName,
                "short_name" => $appName,
                "description" => $appName,
                "lang" => "en-US",
                "start_url" => "/",
                "display" => "standalone",
                "background_color" => $backgroundColor,
                "theme_color" => $themeColor,
                "icons" => $iconPaths
            ];

            file_put_contents('../manifest.json', json_encode($manifest, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));

            // Tạo file sw.js
            $serviceWorker = "self.addEventListener('push', function(event) {\n" .
                "  const data = event.data.json();\n" .
                "  const options = {\n" .
                "    body: data.body,\n" .
                "    icon: data.icon || '/assest-src/512x512.png',\n" .
                "    data: { url: data.url }\n" .
                "  };\n" .
                "  event.waitUntil(self.registration.showNotification(data.title, options));\n" .
                "});\n\n" .
                "self.addEventListener('notificationclick', function(event) {\n" .
                "  event.notification.close();\n" .
                "  const url = event.notification.data.url;\n" .
                "  event.waitUntil(clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function(clientList) {\n" .
                "    let matchingClient = null;\n" .
                "    for (let i = 0; i < clientList.length; i++) {\n" .
                "      const client = clientList[i];\n" .
                "      if (client.url === url && 'focus' in client) {\n" .
                "        matchingClient = client;\n" .
                "        break;\n" .
                "      }\n" .
                "    }\n" .
                "    if (matchingClient) {\n" .
                "      return matchingClient.navigate(url).then(function(client) { return client.focus(); });\n" .
                "    } else {\n" .
                "      return clients.openWindow(url);\n" .
                "    }\n" .
                "  }));\n" .
                "});";

            file_put_contents('../sw.js', $serviceWorker);

            // Tạo file .evn
            $createenv = "NAME='".$appName."'\nDB_TYPE=".$DB_TYPE."\nDB_HOST=".$DB_HOST."\nDB_PORT=".$DB_PORT."\nDB_DATABASE=".$DB_DATABASE."\nDB_USERNAME=".$DB_USERNAME."\nDB_PASSWORD=".$DB_PASSWORD."\nDB_CHARSET=".$DB_CHARSET."\nDB_COLLATION=".$DB_COLLATION."\nDB_PREFIX=".$DB_PREFIX."\nDB_LOGGING=true\nDB_ERROR=ERRMODE_SILENT\nNOTI_PUBLIC_KEY=BD81UCTnmqc5XgzIPDo_kJtcBrwIOlb9lIN7zpEX7OB5fOMfU6aQgrCdGaSF4qAZw3gSnNJ4v9aI5eCvxyWPpYo\nNOTI_PRIVATE_KEY=0bi2pkOCPLyVfFstZ1vhK33-AJeOudbj5Nk7BoE_uxs\nURL=".$websiteURL;
            file_put_contents('../.env', $createenv);

            // Tạo các splash screen cho iOS và Android
            $splashScreens = [
                'android' => [
                    '320x480', '480x800', '720x1280', '1080x1920'
                ],
                'ios' => [
                    '640x960', '640x1136', '750x1334', '1125x2436', '1242x2208', '1536x2048', '2048x2732'
                ]
            ];
            // Hàm chuyển đổi mã màu hex thành RGB
            // function hexToRgb($hex) {
            //     $hex = ltrim($hex, '#');
            //     $length = strlen($hex);

            //     if ($length == 3) {
            //         $r = hexdec($hex[0] . $hex[0]);
            //         $g = hexdec($hex[1] . $hex[1]);
            //         $b = hexdec($hex[2] . $hex[2]);
            //     } elseif ($length == 6) {
            //         $r = hexdec($hex[0] . $hex[1]);
            //         $g = hexdec($hex[2] . $hex[3]);
            //         $b = hexdec($hex[4] . $hex[5]);
            //     } else {
            //         return null; // Mã hex không hợp lệ
            //     }

            //     return ['r' => $r, 'g' => $g, 'b' => $b];
            // }

            // Tạo các file splash screen cho Android và iOS
            // foreach (['android', 'ios'] as $platform) {
            //     foreach ($splashScreens[$platform] as $size) {
            //         $splashFile = $uploadDir . "{$platform}_splash_{$size}.png";
                    
            //         // Phân tách kích thước của splash screen
            //         list($width, $height) = explode('x', $size);

            //         // Tạo ảnh splash mới với nền màu theo backgroundColor
            //         $dstSplash = imagecreatetruecolor($width, $height);
            //         imagesavealpha($dstSplash, true);
            //         $bgColor = hexToRgb($backgroundColor); // Chuyển hex thành RGB
            //         $background = imagecolorallocate($dstSplash, $bgColor['r'], $bgColor['g'], $bgColor['b']);
            //         imagefill($dstSplash, 0, 0, $background);

            //         // Resize icon để giữ nguyên tỷ lệ
            //         $icon = imagecreatefrompng($uploadedFile);
            //         list($iconWidth, $iconHeight) = getimagesize($uploadedFile);

            //         // Giảm kích thước icon xuống 50% so với kích thước splash screen mà vẫn giữ tỷ lệ
            //         $scaleFactor = min($width, $height) * 0.5 / max($iconWidth, $iconHeight); // Tính tỷ lệ thu nhỏ
            //         $newIconWidth = $iconWidth * $scaleFactor;
            //         $newIconHeight = $iconHeight * $scaleFactor;

            //         // Tạo ảnh icon đã thay đổi kích thước
            //         $iconResized = imagescale($icon, $newIconWidth, $newIconHeight);

            //         // Tính toán vị trí căn giữa của icon
            //         $x = ($width - $newIconWidth) / 2;
            //         $y = ($height - $newIconHeight) / 2;

            //         // Vẽ icon vào splash screen
            //         imagecopy($dstSplash, $iconResized, $x, $y, 0, 0, $newIconWidth, $newIconHeight);

            //         // Lưu splash screen
            //         imagepng($dstSplash, $splashFile);
            //         imagedestroy($dstSplash);
            //         imagedestroy($iconResized);
            //         $splashHtml .= '<link rel="apple-touch-startup-image" media="screen and (device-width: ' . $width . 'px) and (device-height: ' . $height . 'px)" href="/assest-src/' . basename($splashFile) . '">' . PHP_EOL;

            //     }
            // }
            // Xuất HTML kết quả dễ copy vào
            $htmlResult = "<html lang='en'><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width, initial-scale=1.0'><title>Generated Manifest and Splash Screens</title></head><body>";
            $htmlResult .= "<h1>Manifest and Splash Screens Created Successfully</h1>";
            $htmlResult .= "<h2>Manifest Links:</h2><pre>" . htmlspecialchars(file_get_contents('../manifest.json')) . "</pre>";
            $htmlResult .= "<h2>Service Worker (sw.js):</h2><pre>" . htmlspecialchars(file_get_contents('../sw.js')) . "</pre>";
            $htmlResult .= "<h2>Splash Screens:</h2><pre>" . htmlspecialchars($splashHtml) . "</pre>";
            foreach (['android', 'ios'] as $platform) {
                $htmlResult .= "<h3>" . ucfirst($platform) . "</h3>";
                foreach ($splashScreens[$platform] as $size) {
                    $htmlResult .= "<p><a href='/assest-src/{$platform}_splash_{$size}.png'>Splash Screen {$size} for {$platform}</a></p>";
                }
            }
            $htmlResult .= "</body></html>";

            echo $htmlResult;
            file_put_contents($uploadDir.'assest-src.txt', $splashHtml);

        // } else {
        //     echo "Không thể tải file.";
        // }
    // } else {
    //     echo "Lỗi upload: " . $iconFile['error'];
    // }
} else {
?>
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>ECLO Install</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH" crossorigin="anonymous">
  </head>
  <body class="bg-light">
    <div class="container">
        <form method="POST" enctype="multipart/form-data" style="display:500px">
            <div class="row justify-content-center">
                <div class="col-lg-6">
                    <h1>Install ECLO APP</h1>
                    <div class="mb-2">
                        <label>Tên App:</label>
                        <input type="text" name="app_name" required class="form-control rounded-3 py-3">
                    </div>
                    <div class="mb-2">
                        <label>Tên Identifier:</label>
                        <input type="text" name="app_identifier" required class="form-control rounded-3 py-3">
                    </div>
                    <div class="mb-2">
                        <label>URL Website:</label>
                        <input type="text" name="website_url" required class="form-control rounded-3 py-3">
                    </div>
                    <div class="row">
                        <div class="col-6 mb-2">
                            <label>Theme color:</label>
                            <input type="color" name="theme_color" required class="form-control form-control-color rounded-3 ">
                        </div>
                        <div class="col-6 mb-2">
                            <label>Background color:</label>
                            <input type="color" name="background_color" required class="form-control form-control-color rounded-3 ">
                        </div>
                    </div>
                    <div class="mb-2">
                        <label>Upload Icon (1024x1024):</label>
                        <input type="file" name="icon"  class="form-control rounded-3 py-3">
                    </div>
                    <div class="mb-2">
                        <label>Database Type:</label>
                        <input type="text" name="DB_TYPE" required class="form-control rounded-3 py-3" value="mysql">
                    </div>
                    <div class="mb-2">
                        <label>Database Host:</label>
                        <input type="text" name="DB_HOST" required class="form-control rounded-3 py-3" value="localhost">
                    </div>
                    <div class="mb-2">
                        <label>Database Port:</label>
                        <input type="text" name="DB_PORT" required class="form-control rounded-3 py-3" value="3306">
                    </div>
                    <div class="mb-2">
                        <label>Database Name:</label>
                        <input type="text" name="DB_DATABASE" required class="form-control rounded-3 py-3" value="">
                    </div>
                    <div class="mb-2">
                        <label>Database User:</label>
                        <input type="text" name="DB_USERNAME" required class="form-control rounded-3 py-3" value="">
                    </div>
                    <div class="mb-2">
                        <label>Database Password:</label>
                        <input type="text" name="DB_PASSWORD"  class="form-control rounded-3 py-3" value="">
                    </div>
                    <div class="mb-2">
                        <label>Database Charset:</label>
                        <input type="text" name="DB_CHARSET" required class="form-control rounded-3 py-3" value="utf8mb4">
                    </div>
                    <div class="mb-2">
                        <label>Database Collation:</label>
                        <input type="text" name="DB_COLLATION" required class="form-control rounded-3 py-3" value="utf8mb4_general_ci">
                    </div>
                    <div class="mb-2">
                        <label>Database Prefix:</label>
                        <input type="text" name="DB_PREFIX" class="form-control rounded-3 py-3" value="">
                    </div>

                    <button type="submit" class="btn btn-primary py-3 w-100">Install</button>
                </div>
            </div>
        </form>
    </div>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js" integrity="sha384-YvpcrYf0tY3lHB60NNkmXc5s9fDVZLESaAA55NDzOxhy9GkcIdslK1eN7N6jIeHz" crossorigin="anonymous"></script>
  </body>
</html>
<?php } ?>
