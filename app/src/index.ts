/* @refresh reload */
import { render } from 'solid-js/web'
import h from 'solid-js/h'
import App from './App.tsx'
import './index.css'

const root = document.getElementById('root')

render(h(App), root!)
