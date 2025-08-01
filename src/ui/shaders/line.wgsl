struct VertexInput {
    @location(0) position: vec2<f32>,
    @location(1) strip_index: u32,
};

struct VertexOutput {
    @builtin(position) position: vec4<f32>,
    @location(0) color: vec4<f32>,
    @location(1) uv: vec2<f32>,
    @location(2) line_width: f32,
};

struct Uniforms {
    view_projection: mat4x4<f32>,
    line_width: f32,
    line_color: vec4<f32>,
};

@group(0) @binding(0) var<uniform> uniforms: Uniforms;

@vertex
fn vs_main(input: VertexInput) -> VertexOutput {
    let world_pos = vec4<f32>(input.position, 0.0, 1.0);
    let clip_pos = uniforms.view_projection * world_pos;
    
    return VertexOutput(
        clip_pos,
        uniforms.line_color,
        vec2<f32>(0.0, 0.0), // UV coordinates for line
        uniforms.line_width,
    );
}

@fragment
fn fs_main(input: VertexOutput) -> @location(0) vec4<f32> {
    // Simple anti-aliased line rendering
    let distance = length(input.uv);
    let alpha = 1.0 - smoothstep(0.0, 1.0, distance);
    
    return vec4<f32>(input.color.rgb, input.color.a * alpha);
} 