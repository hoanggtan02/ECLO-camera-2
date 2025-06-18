<?php
    define('ECLO', true);
    require 'vendor/autoload.php';
    ini_set('display_errors', 1);
    ini_set('display_startup_errors', 1);
    error_reporting(E_ALL);
    ob_start();
    session_start();
    require_once 'controllers/includes/config.php';
    $app->run();
?>