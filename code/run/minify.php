<?php
    require '../vendor/autoload.php';
    use ECLO\App;
    $app = new App();
    $css = [
        "../templates/assets/css/bootstrap.min.css",
        "../templates/assets/css/datatables.min.css",
        "../templates/assets/css/style.css",
    ];
    $Js = [
        "../templates/assets/js/jquery-3.7.1.min.js",
        "../templates/assets/js/bootstrap.bundle.min.js",
        "../templates/assets/js/crypto-js.min.js",
        "../templates/assets/js/sweetalert2.all.min.js",
        "../templates/assets/js/lazysizes.min.js",
        "../templates/assets/js/ls.bgset.min.js",
        "../templates/assets/js/infinite-ajax-scroll.min.js",
        "../templates/assets/js/pjax.min.js",
        "../templates/assets/js/topbar.min.js",
        "../templates/assets/js/datatables.min.js",
        "../templates/assets/js/main.js",
    ];
    $app->minifyCSS($css,'../templates/assets/css/style.bundle.css');
	$app->minifyJS($Js,'../templates/assets/js/main.bundle.js');
	// $minifier = new Minify\CSS($sourcePath);
    echo 'Success';
?>