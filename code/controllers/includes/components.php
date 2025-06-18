<?php
    if (!defined('ECLO')) die("Hacking attempt");
    $jatbi = new Jatbi($app);
    $setting = $app->getValueData('setting');
    $SelectMenu = $app->getValueData('menu');
    $app->setComponent('header', function($vars) use ($app, $setting, $jatbi) {
        $account = [];
        if($app->getSession("accounts")){
            $account = $app->get("accounts","*",["id"=>$app->getSession("accounts")['id'],"status"=>"A"]);
        }
        require_once('templates/components/header.html'); 
    });
    // Footer Component
    $app->setComponent('footer', function($vars) use ($app, $setting, $jatbi) {
        require_once('templates/components/footer.html');
    }); 
    //Sidebar Component
    $app->setComponent('sidebar', function($vars) use ($app, $setting, $jatbi,$SelectMenu) {
        if($app->getSession("accounts")){
            $account = $app->get("accounts","*",["id"=>$app->getSession("accounts")['id'],"status"=>"A"]);
            $getsetting = $app->get("settings","*",["account"=>$account['id']]);
            $notification = $app->count("notifications","id",["account"=>$account['id'],"views"=>0]);
            $getRouter = explode("/",$app->getRoute());
        }
        require_once('templates/components/sidebar.html');
    });
    //status Component
    $app->setComponent('status', function($vars) use ($app, $setting, $jatbi) {
        $url = isset($vars['url']) ? $vars['url'] : '';
        $data = isset($vars['data']) ? $vars['data'] : '';
        $permissions = isset($vars['permission']) ? $vars['permission'] : [];
        $hasPermission = empty($permissions) || array_reduce($permissions, fn($carry, $perm) => $carry || $jatbi->permission($perm) == 'true', false);
        if ($hasPermission) {
            echo '<div class="form-check form-switch">
                  <input class="form-check-input" data-action="click" data-url="'.$url.'" data-alert="true" type="checkbox" role="switch" ' . ($data=='A' ? 'checked' : '') . '>
               </div>';
        }
        else {
            echo '<div class="form-check form-switch">
                  <input class="form-check-input" disabled type="checkbox" role="switch" ' . ($data=='A' ? 'checked' : '') . '>
               </div>';
        }
    });
    //box Component
    $app->setComponent('box', function($vars) use ($app, $setting, $jatbi) {
        $data = isset($vars['data']) ? $vars['data'] : '';
        echo '<div class="form-check"><input class="form-check-input checker" type="checkbox" value="'.$data.'"></div>';
    });
    //action Component
    $app->setComponent('action', function($vars) use ($app, $jatbi) {
        if (!is_array($vars) || !isset($vars['button']) || !is_array($vars['button'])) {
            return;
        }
        $buttons = $vars['button'];
        $class = isset($vars['class']) ? $vars['class'] : '';
        $output = '';
        foreach ($buttons as $button) {
            if (!is_array($button) || !isset($button['type'])) {
                continue;
            }
            $name = htmlspecialchars($button['name'] ?? '');
            $icon = $button['icon'] ?? '';
            $class = htmlspecialchars($button['class'] ?? '');
            $type = $button['type'] ?? 'link';
            $permissions = $button['permission'] ?? [];
            $action = isset($button['action']) && is_array($button['action']) 
                ? implode(' ', array_map(fn($key, $value) => htmlspecialchars($key) . '="' . htmlspecialchars($value) . '"', array_keys($button['action']), $button['action']))
                : '';
            $hasPermission = empty($permissions) || array_reduce($permissions, fn($carry, $perm) => $carry || $jatbi->permission($perm) == 'true', false);
            if ($hasPermission) {
                if ($type === 'button') { 
                    $output .= '<li><button ' . $action . ' class="btn dropdown-item ' . $class . '">'.$icon . $name . '</button></li>';
                }
                elseif ($type === 'divider') { 
                    $output .= '<li><hr class="dropdown-divider"></li>';
                } else {
                    $output .= '<li><a ' . $action . ' class="btn dropdown-item ' . $class . '">'.$icon . $name . '</a></li>';
                }
            }
        }
        if (empty($output)) {
            return;
        }
        echo '<div class="dropdown">
                <button class="btn btn-primary-light btn-sm border-0 py-1 px-2 rounded-3 small fw-bold fs-6 '.$class.'" type="button" data-bs-toggle="dropdown" aria-expanded="false">
                    <i class="ti ti-dots"></i>
                </button>
                <ul class="dropdown-menu dropdown-menu-end border-0 bg-body shadow-lg rounded-4 min-width" style="--min-width:100px">'
                . $output .
                '</ul>
            </div>';
    });


    // Checkbox Component
    $app->setComponent('checkbox', function($vars) {
        $name = isset($vars['name']) ? $vars['name'] : '';
        $label = isset($vars['label']) ? $vars['label'] : '';
        $checked = isset($vars['checked']) ? 'checked' : '';
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '
        <div class="form-check mb-3' . $class . '"' . $id . $attr . '>
            <input class="form-check-input" type="checkbox" id="' . htmlspecialchars($name) . '" name="' . htmlspecialchars($name) . '" ' . $checked . '>
            <label class="form-check-label" for="' . htmlspecialchars($name) . '">
                ' . htmlspecialchars($label) . '
            </label>
        </div>';
    });
    // Input Component
    $app->setComponent('input', function($vars) {
        $type = isset($vars['type']) ? $vars['type'] : 'text';
        $name = isset($vars['name']) ? $vars['name'] : '';
        $value = isset($vars['value']) ? $vars['value'] : '';
        $placeholder = isset($vars['placeholder']) ? $vars['placeholder'] : '';
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '
        <div class="mb-3">
            <label for="' . htmlspecialchars($name) . '" class="form-label">' . htmlspecialchars($placeholder) . '</label>
            <input type="' . htmlspecialchars($type) . '" class="form-control rounded-4 p-3 ' . $class . '"' . $id . ' name="' . htmlspecialchars($name) . '" value="' . htmlspecialchars($value) . '" placeholder="' . htmlspecialchars($placeholder) . '"' . $attr . '>
        </div>';
    });

    // Textarea Component
    $app->setComponent('textarea', function($vars) {
        $type = isset($vars['type']) ? $vars['type'] : 'text';
        $name = isset($vars['name']) ? $vars['name'] : '';
        $value = isset($vars['value']) ? $vars['value'] : '';
        $placeholder = isset($vars['placeholder']) ? $vars['placeholder'] : '';
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '
        <div class="mb-3">
            <label for="' . htmlspecialchars($name) . '" class="form-label">' . htmlspecialchars($placeholder) . '</label>
            <textarea type="' . htmlspecialchars($type) . '" class="form-control rounded-4 p-3 ' . $class . '"' . $id . ' name="' . htmlspecialchars($name) . '" placeholder="' . htmlspecialchars($placeholder) . '"' . $attr . '>' . htmlspecialchars($value) . '</textarea>
        </div>';
    });
    // Button Component
    $app->setComponent('button', function($vars) {
        $label = isset($vars['label']) ? $vars['label'] : 'Click Me';
        $type = isset($vars['type']) ? $vars['type'] : 'button';
        $color = isset($vars['color']) ? $vars['color'] : 'danger';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '
            <button type="'.$type.'" class="btn rounded-pill btn-' . htmlspecialchars($color) . $class . '"' . $id . $attr . '>' . ($label) . '</button>
        ';
    });

    // Alert Component
    $app->setComponent('alert', function($vars) {
        $message = isset($vars['message']) ? $vars['message'] : 'Alert message here.';
        $type = isset($vars['type']) ? $vars['type'] : 'info';
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '
            <div class="alert alert-' . htmlspecialchars($type) . ' alert-dismissible fade show' . $class . '"' . $id . $attr . ' role="alert">
                ' . htmlspecialchars($message) . '
                <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
            </div>';
    });

    // Badge Component
    $app->setComponent('badge', function($vars) {
        $text = isset($vars['text']) ? $vars['text'] : 'Badge';
        $type = isset($vars['type']) ? $vars['type'] : 'secondary';
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';
        echo '<span class="badge bg-' . htmlspecialchars($type) . $class . '"' . $id . $attr . '>' . htmlspecialchars($text) . '</span>';
    });

    // Dropdown Component
    $app->setComponent('dropdown', function($vars) {
        $id = isset($vars['id']) ? $vars['id'] : 'dropdownMenuButton';
        $items = isset($vars['items']) ? $vars['items'] : [];
        $label = isset($vars['label']) ? $vars['label'] : 'Dropdown';
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '<div class="dropdown' . $class . '"' . $attr . '>
            <button class="btn btn-secondary dropdown-toggle" type="button" id="' . htmlspecialchars($id) . '" data-bs-toggle="dropdown" aria-expanded="false">
                ' . htmlspecialchars($label) . '
            </button>
            <ul class="dropdown-menu" aria-labelledby="' . htmlspecialchars($id) . '">';
            foreach ($items as $item) {
                echo '<li><a class="dropdown-item" href="' . htmlspecialchars($item['href']) . '">' . htmlspecialchars($item['text']) . '</a></li>';
            }
        echo '</ul>
        </div>';
    });

    // Card Component
    $app->setComponent('card', function($vars) {
        $title = isset($vars['title']) ? $vars['title'] : 'Card Title';
        $body = isset($vars['body']) ? $vars['body'] : 'Card body content here.';
        $image = isset($vars['image']) ? $vars['image'] : 'https://via.placeholder.com/150';
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '
        <div class="card' . $class . '"' . $id . $attr . ' style="width: 18rem;">
            <img src="' . htmlspecialchars($image) . '" class="card-img-top" alt="...">
            <div class="card-body">
                <h5 class="card-title">' . htmlspecialchars($title) . '</h5>
                <p class="card-text">' . htmlspecialchars($body) . '</p>
                <a href="#" class="btn btn-primary">Go somewhere</a>
            </div>
        </div>';
    });

    // Accordion Component
    $app->setComponent('accordion', function($vars) {
        $items = isset($vars['items']) ? $vars['items'] : [];
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '<div class="accordion' . $class . '"' . $id . $attr . '>';
            foreach ($items as $index => $item) {
                $isActive = $index === 0 ? 'show' : '';
                echo '
                <div class="accordion-item">
                    <h2 class="accordion-header" id="heading' . $index . '">
                        <button class="accordion-button ' . ($index !== 0 ? 'collapsed' : '') . '" type="button" data-bs-toggle="collapse" data-bs-target="#collapse' . $index . '" aria-expanded="' . ($index === 0 ? 'true' : 'false') . '" aria-controls="collapse' . $index . '">
                            ' . htmlspecialchars($item['header']) . '
                        </button>
                    </h2>
                    <div id="collapse' . $index . '" class="accordion-collapse collapse ' . $isActive . '" aria-labelledby="heading' . $index . '" data-bs-parent="#accordionExample">
                        <div class="accordion-body">
                            ' . htmlspecialchars($item['body']) . '
                        </div>
                    </div>
                </div>';
            }
        echo '</div>';
    });

    // Button Group Component
    $app->setComponent('button-group', function($vars) {
        $buttons = isset($vars['buttons']) ? $vars['buttons'] : [];
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '<div class="btn-group' . $class . '"' . $id . $attr . ' role="group" aria-label="Button group">';
            foreach ($buttons as $button) {
                echo '<button type="button" class="btn btn-' . htmlspecialchars($button['type']) . '">' . htmlspecialchars($button['label']) . '</button>';
            }
        echo '</div>';
    });

    // Input Group Component
    $app->setComponent('input-group', function($vars) {
        $inputs = isset($vars['inputs']) ? $vars['inputs'] : [];
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '<div class="input-group' . $class . '"' . $id . $attr . '>';
            foreach ($inputs as $input) {
                echo '<span class="input-group-text">' . htmlspecialchars($input['text']) . '</span>';
                echo '<input type="' . htmlspecialchars($input['type']) . '" class="form-control p-3" placeholder="' . htmlspecialchars($input['placeholder']) . '">';
            }
        echo '</div>';
    });

    // Input Group Component
    $app->setComponent('input-group-button', function($vars) {

        $button_label = isset($vars['button']['label']) ? $vars['button']['label'] : 'Click Me';
        $button_type = isset($vars['button']['type']) ? $vars['button']['type'] : 'danger';
        $button_color = isset($vars['button']['color']) ? $vars['button']['color'] : 'danger';
        $button_id = isset($vars['button']['id']) ? ' id="' . htmlspecialchars($vars['button']['id']) . '"' : '';
        $button_class = isset($vars['button']['class']) ? ' ' . htmlspecialchars($vars['button']['class']) : '';
        $button_attr = isset($vars['button']['attr']) ? $vars['button']['attr'] : '';

        $type = isset($vars['type']) ? $vars['type'] : 'text';
        $name = isset($vars['name']) ? $vars['name'] : '';
        $value = isset($vars['value']) ? $vars['value'] : '';
        $placeholder = isset($vars['placeholder']) ? $vars['placeholder'] : '';
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '
            <div class="mb-3">
                <label for="' . htmlspecialchars($name) . '" class="form-label">' . htmlspecialchars($placeholder) . '</label>
                <div class="input-group">
                    <input type="' . htmlspecialchars($type) . '" class="form-control p-3 ' . $class . '"' . $id . ' name="' . htmlspecialchars($name) . '" value="' . htmlspecialchars($value) . '" placeholder="' . htmlspecialchars($placeholder) . '"' . $attr . '>
                    <button type="'.$button_type.'" class="btn btn-' . htmlspecialchars($button_color) . $button_class . '"' . $button_id . $button_attr . '>' . ($button_label) . '</button>
                </div>
            </div>';
    });
    // List Group Component
    $app->setComponent('list-group', function($vars) {
        $items = isset($vars['items']) ? $vars['items'] : [];
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '<ul class="list-group' . $class . '"' . $id . $attr . '>';
            foreach ($items as $item) {
                echo '<li class="list-group-item">' . htmlspecialchars($item['text']) . '</li>';
            }
        echo '</ul>';
    });

    // Nav Component
    $app->setComponent('nav', function($vars) {
        $items = isset($vars['items']) ? $vars['items'] : [];
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '<nav class="navbar navbar-expand-lg navbar-light bg-light' . $class . '"' . $id . $attr . '>
            <div class="container-fluid">
                <a class="navbar-brand" href="#">Navbar</a>
                <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav" aria-controls="navbarNav" aria-expanded="false" aria-label="Toggle navigation">
                    <span class="navbar-toggler-icon"></span>
                </button>
                <div class="collapse navbar-collapse" id="navbarNav">
                    <ul class="navbar-nav">';
                        foreach ($items as $item) {
                            echo '<li class="nav-item">
                                <a class="nav-link" href="' . htmlspecialchars($item['href']) . '">' . htmlspecialchars($item['text']) . '</a>
                            </li>';
                        }
        echo '
                    </ul>
                </div>
            </div>
        </nav>';
    });

    // Progress Component
    $app->setComponent('progress', function($vars) {
        $value = isset($vars['value']) ? $vars['value'] : 0;
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '
        <div class="progress' . $class . '"' . $id . $attr . '>
            <div class="progress-bar" role="progressbar" style="width: ' . htmlspecialchars($value) . '%;" aria-valuenow="' . htmlspecialchars($value) . '" aria-valuemin="0" aria-valuemax="100">' . htmlspecialchars($value) . '%</div>
        </div>';
    });

    // Toast Component
    $app->setComponent('toast', function($vars) {
        $message = isset($vars['message']) ? $vars['message'] : 'Toast message here.';
        $type = isset($vars['type']) ? $vars['type'] : 'success';
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '
        <div class="toast align-items-center text-white bg-' . htmlspecialchars($type) . ' border-0' . $class . '"' . $id . $attr . ' role="alert" aria-live="assertive" aria-atomic="true">
            <div class="d-flex">
                <div class="toast-body">
                    ' . htmlspecialchars($message) . '
                </div>
                <button type="button" class="btn-close btn-close-white" data-bs-dismiss="toast" aria-label="Close"></button>
            </div>
        </div>';
    });

    // Select Component
    $app->setComponent('select', function($vars) {
        $name = isset($vars['name']) ? $vars['name'] : '';
        $options = isset($vars['options']) ? $vars['options'] : [];
        $selected = isset($vars['selected']) ? $vars['selected'] : '';
        $placeholder = isset($vars['placeholder']) ? $vars['placeholder'] : '';
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '
        <div class="mb-3">
            <label for="' . htmlspecialchars($name) . '" class="form-label">'.$placeholder.'</label>
            <select data-select data-style="form-select bg-body-tertiary py-3 rounded-4 w-100" data-live-search="true"  class="'  . $class . '"' . $id . ' name="' . htmlspecialchars($name) . '"' . $attr . '>';
            foreach ($options as $option) {
                echo '<option value="' . htmlspecialchars($option['value']) . '"' . ($selected == $option['value'] ? ' selected' : '') . '>' . htmlspecialchars($option['text']) . '</option>';
            }
        echo '
            </select>
        </div>';
    });

    // Radio Component
    $app->setComponent('radio', function($vars) {
        $name = isset($vars['name']) ? $vars['name'] : '';
        $options = isset($vars['options']) ? $vars['options'] : [];
        $selected = isset($vars['selected']) ? $vars['selected'] : '';
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        foreach ($options as $option) {
            echo '
            <div class="form-check mb-3' . $class . '"' . $id . $attr . '>
                <input class="form-check-input" type="radio" id="' . htmlspecialchars($option['value']) . '" name="' . htmlspecialchars($name) . '" value="' . htmlspecialchars($option['value']) . '"' . ($selected == $option['value'] ? ' checked' : '') . '>
                <label class="form-check-label" for="' . htmlspecialchars($option['value']) . '">
                    ' . htmlspecialchars($option['text']) . '
                </label>
            </div>';
        }
    });

    // Table Component
    $app->setComponent('table', function($vars) {
        $headers = isset($vars['headers']) ? $vars['headers'] : [];
        $rows = isset($vars['rows']) ? $vars['rows'] : [];
        $class = isset($vars['class']) ? ' ' . htmlspecialchars($vars['class']) : '';
        $id = isset($vars['id']) ? ' id="' . htmlspecialchars($vars['id']) . '"' : '';
        $attr = isset($vars['attr']) ? $vars['attr'] : '';

        echo '
        <table class="table' . $class . '"' . $id . $attr . '>
            <thead>
                <tr>';
                foreach ($headers as $header) {
                    echo '<th scope="col">' . htmlspecialchars($header) . '</th>';
                }
        echo '
                </tr>
            </thead>
            <tbody>';
            foreach ($rows as $row) {
                echo '<tr>';
                foreach ($row as $cell) {
                    echo '<td>' . htmlspecialchars($cell) . '</td>';
                }
                echo '</tr>';
            }
        echo '
            </tbody>
        </table>';
    });
?>