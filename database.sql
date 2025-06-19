-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Máy chủ: localhost
-- Thời gian đã tạo: Th3 10, 2025 lúc 03:00 PM
-- Phiên bản máy phục vụ: 5.7.44-log
-- Phiên bản PHP: 8.3.12

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Cơ sở dữ liệu: `ellm_demo`
--

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `accounts`
--

CREATE TABLE `accounts` (
  `id` int(11) NOT NULL,
  `type` int(11) DEFAULT NULL,
  `name` varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL,
  `account` varchar(300) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `phone` varchar(300) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `email` varchar(300) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `password` mediumtext COLLATE utf8mb4_unicode_ci,
  `content` varchar(3000) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `active` mediumtext COLLATE utf8mb4_unicode_ci NOT NULL,
  `avatar` mediumtext COLLATE utf8mb4_unicode_ci,
  `birthday` date DEFAULT NULL,
  `gender` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `date` datetime NOT NULL,
  `status` varchar(1) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'A',
  `permission` int(11) DEFAULT NULL,
  `deleted` int(11) DEFAULT '0',
  `forget` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `login` varchar(300) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `login_data` varchar(3000) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `online` int(11) NOT NULL DEFAULT '0',
  `root` int(11) NOT NULL DEFAULT '0',
  `lang` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'vi'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Đang đổ dữ liệu cho bảng `accounts`
--

INSERT INTO `accounts` (`id`, `type`, `name`, `account`, `phone`, `email`, `password`, `content`, `active`, `avatar`, `birthday`, `gender`, `date`, `status`, `permission`, `deleted`, `forget`, `login`, `login_data`, `online`, `root`, `lang`) VALUES
(1, 1, 'Jatbi', 'jatbirat', '0939330014', 'jatbirat@gmail.com', '$2y$10$P8j3uxx8IKITE993FlCcAOb1qxOODrbKWLdQ08k72i1PW6TLL.nMe', NULL, '56b938fe-6b8b-4ba9-9f89-1e02fb591198', 'upload/images/0fcb023e-457d-4579-a339-f8736a173c3b', '2025-03-04', '2', '2025-03-04 21:50:54', 'A', 1, 0, NULL, 'create', NULL, 0, 0, 'vi'),
(2, 1, 'Demo', 'demo', '', 'demo@eclo.vn', '$2y$10$6ivk7P.HOPDAzXDT27ELQexggVH4JFgQB3pNbbadu6infn0BmT/QO', NULL, '11df4d57-5768-4f94-9fbf-d0b3b33f4fd3', 'upload/images/663d6a4b-fe02-4511-8f01-fe201ca9b9f8', '0000-00-00', '1', '2025-03-09 16:11:14', 'A', 1, 0, NULL, 'create', NULL, 0, 0, 'vi');

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `accounts_login`
--

CREATE TABLE `accounts_login` (
  `id` int(11) NOT NULL,
  `accounts` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `token` varchar(600) COLLATE utf8mb4_unicode_ci NOT NULL,
  `notification` longtext COLLATE utf8mb4_unicode_ci,
  `ip` varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL,
  `agent` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
  `deleted` int(11) NOT NULL DEFAULT '0',
  `date` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `accounts_notification`
--

CREATE TABLE `accounts_notification` (
  `id` int(11) NOT NULL,
  `account` int(11) NOT NULL,
  `endpoint` text NOT NULL,
  `p256dh` text NOT NULL,
  `auth` text NOT NULL,
  `agent` text NOT NULL,
  `deleted` int(11) NOT NULL DEFAULT '0',
  `date` datetime NOT NULL,
  `active` varchar(300) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `accounts_times`
--

CREATE TABLE `accounts_times` (
  `id` int(11) NOT NULL,
  `account` int(11) NOT NULL,
  `fd` int(11) NOT NULL DEFAULT '0',
  `domain` varchar(300) DEFAULT NULL,
  `date` datetime NOT NULL,
  `time` double NOT NULL DEFAULT '0',
  `deleted` int(11) NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `blockip`
--

CREATE TABLE `blockip` (
  `id` int(11) NOT NULL,
  `ip` varchar(300) NOT NULL,
  `status` varchar(1) NOT NULL,
  `date` datetime NOT NULL,
  `active` varchar(300) NOT NULL,
  `deleted` int(11) NOT NULL DEFAULT '0',
  `notes` varchar(3000) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `logs`
--

CREATE TABLE `logs` (
  `id` int(11) NOT NULL,
  `user` int(11) DEFAULT NULL,
  `dispatch` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `action` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `content` mediumtext COLLATE utf8mb4_unicode_ci,
  `date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `url` mediumtext COLLATE utf8mb4_unicode_ci,
  `ip` mediumtext COLLATE utf8mb4_unicode_ci,
  `browsers` mediumtext COLLATE utf8mb4_unicode_ci,
  `deleted` int(11) DEFAULT '0',
  `active` varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `notifications`
--

CREATE TABLE `notifications` (
  `id` int(11) NOT NULL,
  `user` int(11) DEFAULT NULL,
  `account` int(11) DEFAULT NULL,
  `date` datetime NOT NULL,
  `title` varchar(300) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `content` varchar(3000) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `url` varchar(300) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `deleted` int(11) NOT NULL DEFAULT '0',
  `views` int(11) NOT NULL DEFAULT '0',
  `active` varchar(300) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `template` varchar(300) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `fcm` mediumtext COLLATE utf8mb4_unicode_ci,
  `logs` mediumtext COLLATE utf8mb4_unicode_ci,
  `type` varchar(300) COLLATE utf8mb4_unicode_ci DEFAULT 'content',
  `data` varchar(300) COLLATE utf8mb4_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `permissions`
--

CREATE TABLE `permissions` (
  `id` int(11) NOT NULL,
  `name` varchar(300) NOT NULL,
  `permissions` text,
  `status` varchar(1) NOT NULL,
  `active` varchar(300) NOT NULL,
  `deleted` int(11) NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Đang đổ dữ liệu cho bảng `permissions`
--

INSERT INTO `permissions` (`id`, `name`, `permissions`, `status`, `active`, `deleted`) VALUES
(1, 'Quản trị viên', '{\"accounts\":\"accounts\",\"accounts.add\":\"accounts.add\",\"accounts.edit\":\"accounts.edit\",\"accounts.deleted\":\"accounts.deleted\",\"permission\":\"permission\",\"permission.add\":\"permission.add\",\"permission.edit\":\"permission.edit\",\"permission.deleted\":\"permission.deleted\",\"blockip\":\"blockip\",\"blockip.add\":\"blockip.add\",\"blockip.edit\":\"blockip.edit\",\"blockip.deleted\":\"blockip.deleted\",\"config\":\"config\",\"logs\":\"logs\",\"trash\":\"trash\"}', 'A', '48a81d0b-c0e8-4166-8cb3-e6fb007118f6', 0),
(2, 'ok', '{\"accounts\":\"accounts\",\"accounts.add\":\"accounts.add\",\"accounts.edit\":\"accounts.edit\",\"accounts.deleted\":\"accounts.deleted\",\"permission\":\"permission\",\"permission.add\":\"permission.add\",\"permission.edit\":\"permission.edit\",\"permission.deleted\":\"permission.deleted\",\"blockip\":\"blockip\",\"blockip.add\":\"blockip.add\",\"blockip.edit\":\"blockip.edit\",\"blockip.deleted\":\"blockip.deleted\",\"config\":\"config\",\"logs\":\"logs\",\"trash\":\"trash\"}', 'A', '7b850871-8496-422e-aff6-ed26cb5c245a', 0),
(3, 'asdasd', '{\"accounts\":\"accounts\",\"accounts.add\":\"accounts.add\",\"accounts.edit\":\"accounts.edit\",\"accounts.deleted\":\"accounts.deleted\",\"permission\":\"permission\",\"permission.add\":\"permission.add\",\"permission.edit\":\"permission.edit\",\"permission.deleted\":\"permission.deleted\",\"blockip\":\"blockip\",\"blockip.add\":\"blockip.add\",\"blockip.edit\":\"blockip.edit\",\"blockip.deleted\":\"blockip.deleted\",\"config\":\"config\",\"logs\":\"logs\",\"trash\":\"trash\"}', 'A', '3e722113-e438-4291-a991-0dca63117c31', 1);

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `settings`
--

CREATE TABLE `settings` (
  `id` int(11) NOT NULL,
  `account` int(11) NOT NULL,
  `notification` int(11) NOT NULL DEFAULT '1',
  `notification_mail` int(11) NOT NULL DEFAULT '0',
  `api` int(11) NOT NULL DEFAULT '0',
  `access_token` text,
  `deleted` int(11) NOT NULL DEFAULT '0',
  `lang` varchar(30) NOT NULL DEFAULT 'vi'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Đang đổ dữ liệu cho bảng `settings`
--

INSERT INTO `settings` (`id`, `account`, `notification`, `notification_mail`, `api`, `access_token`, `deleted`, `lang`) VALUES
(1, 1, 1, 0, 1, 'zXrsvGTfYf1wbNpikr8pqONXMtDLqJ0LteNURQDCH4sFuNGIF0ZI4p3YOrNrmwuA7LPBZMyzOfmXqdAULTp86fgG3HbOa90NKmJakEmfTUV2knf00J5JYpyskhTeC9oh', 0, 'vi'),
(2, 2, 1, 0, 0, NULL, 0, 'vi');

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `trashs`
--

CREATE TABLE `trashs` (
  `id` int(11) NOT NULL,
  `content` text,
  `account` int(11) NOT NULL,
  `date` datetime NOT NULL,
  `data` text NOT NULL,
  `active` varchar(300) NOT NULL,
  `ip` varchar(300) DEFAULT NULL,
  `url` text,
  `router` text NOT NULL,
  `deleted` int(11) NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `uploads`
--

CREATE TABLE `uploads` (
  `id` int(11) NOT NULL,
  `account` int(11) NOT NULL,
  `type` varchar(300) NOT NULL,
  `content` text NOT NULL,
  `date` datetime NOT NULL,
  `active` varchar(300) NOT NULL,
  `data` text,
  `size` double NOT NULL,
  `mime` varchar(300) DEFAULT NULL,
  `deleted` int(11) NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Đang đổ dữ liệu cho bảng `uploads`
--

INSERT INTO `uploads` (`id`, `account`, `type`, `content`, `date`, `active`, `data`, `size`, `mime`, `deleted`) VALUES
(1, 1, 'images', 'datas/56b938fe-6b8b-4ba9-9f89-1e02fb591198/images/0fcb023e-457d-4579-a339-f8736a173c3b.png', '2025-03-04 21:50:54', '0fcb023e-457d-4579-a339-f8736a173c3b', '{\"file_src_name\":\"avatar5.png\",\"file_src_name_body\":\"avatar5\",\"file_src_name_ext\":\"png\",\"file_src_pathname\":\"datas\\/avatar\\/avatar5.png\",\"file_src_mime\":\"image\\/png\",\"file_src_size\":336040,\"image_src_x\":512,\"image_src_y\":512,\"image_src_pixels\":262144}', 336040, NULL, 0),
(2, 2, 'images', 'datas/11df4d57-5768-4f94-9fbf-d0b3b33f4fd3/images/663d6a4b-fe02-4511-8f01-fe201ca9b9f8.png', '2025-03-09 16:11:14', '663d6a4b-fe02-4511-8f01-fe201ca9b9f8', '{\"file_src_name\":\"avatar4.png\",\"file_src_name_body\":\"avatar4\",\"file_src_name_ext\":\"png\",\"file_src_pathname\":\"datas\\/avatar\\/avatar4.png\",\"file_src_mime\":\"image\\/png\",\"file_src_size\":336339,\"image_src_x\":512,\"image_src_y\":512,\"image_src_pixels\":262144}', 336339, NULL, 0);

--
-- Chỉ mục cho các bảng đã đổ
--

--
-- Chỉ mục cho bảng `accounts`
--
ALTER TABLE `accounts`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `accounts_login`
--
ALTER TABLE `accounts_login`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `accounts_notification`
--
ALTER TABLE `accounts_notification`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `accounts_times`
--
ALTER TABLE `accounts_times`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `blockip`
--
ALTER TABLE `blockip`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `logs`
--
ALTER TABLE `logs`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `notifications`
--
ALTER TABLE `notifications`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `permissions`
--
ALTER TABLE `permissions`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `settings`
--
ALTER TABLE `settings`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `trashs`
--
ALTER TABLE `trashs`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `uploads`
--
ALTER TABLE `uploads`
  ADD PRIMARY KEY (`id`);

--
-- AUTO_INCREMENT cho các bảng đã đổ
--

--
-- AUTO_INCREMENT cho bảng `accounts`
--
ALTER TABLE `accounts`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=3;

--
-- AUTO_INCREMENT cho bảng `accounts_login`
--
ALTER TABLE `accounts_login`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `accounts_notification`
--
ALTER TABLE `accounts_notification`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `accounts_times`
--
ALTER TABLE `accounts_times`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `blockip`
--
ALTER TABLE `blockip`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `logs`
--
ALTER TABLE `logs`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `notifications`
--
ALTER TABLE `notifications`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `permissions`
--
ALTER TABLE `permissions`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=4;

--
-- AUTO_INCREMENT cho bảng `settings`
--
ALTER TABLE `settings`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=3;

--
-- AUTO_INCREMENT cho bảng `trashs`
--
ALTER TABLE `trashs`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `uploads`
--
ALTER TABLE `uploads`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=3;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
